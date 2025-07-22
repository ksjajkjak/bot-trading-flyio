
import os
import time
import pandas as pd
import threading
from websocket import WebSocketApp
from datetime import datetime
import math
from binance.client import Client
from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET
import logging

# Configurar logging para mejor debugging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from collections import deque

# CONFIGURACIÓN BINANCE
API_KEY = os.environ.get('BINANCE_API_KEY')
SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY')

if not API_KEY or not SECRET_KEY:
    raise ValueError("ERROR: Configura BINANCE_API_KEY y BINANCE_SECRET_KEY en Secrets")

client = Client(API_KEY, SECRET_KEY)

# CONFIGURACIÓN DE TRADING (optimizado para $1.8 USDT)
SYMBOL = "SOLUSDT"
EMA_SHORT = 7
EMA_MEDIUM = 25
EMA_LONG = 99

# Configuraciones para PnL (como tu backtest exitoso)
TAKE_PROFIT_PCT = 0.005   # 0.5% (tu mejor backtest)
STOP_LOSS_PCT = 0.005     # 0.5% (tu mejor backtest)
LEVERAGE = 50             # Como pediste para $1.8 USDT
DISTANCIA_MAX = 0.02      # 2% máximo de distancia a EMA25

# NUEVA: Configuración de Trailing Stop
TRAILING_STOP_ENABLED = True
TRAILING_DISTANCE_PCT = 0.003  # 0.3% de distancia para trailing

# Variables globales para WebSocket
klines_1m = deque(maxlen=200)  # Buffer de velas 1m
klines_5m = deque(maxlen=200)  # Buffer de velas 5m
last_5m_close_time = 0
last_1m_close_time = 0

# Variables de posición
position_open = False
current_position = None
entry_price = 0.0
tp_price = 0.0
sl_price = 0.0
position_side = None
entry_timestamp = None
trailing_sl_price = 0.0  # NUEVO: Para trailing stop

# MEJORA 1: Variable para calcular pendiente de EMA99
previous_ema99_5m = None

# Protecciones
MAX_TRADES_PER_DAY = 8
trades_today = 0
last_day_reset = time.time()

# Variables para filtros de símbolo
symbol_info = None
min_qty = 0.0
max_qty = 0.0
step_size = 0.0
min_notional = 2.0  # Futures permite desde $2 USDT con apalancamiento
tick_size = 0.0

# NUEVO: Variable para controlar reconexión (más conservador)
websocket_connected = False
reconnection_attempts = 0
max_reconnection_attempts = 5  # Reducido de 10 a 5

def get_symbol_info():
    """Obtener información del símbolo para validaciones"""
    global symbol_info, min_qty, max_qty, step_size, min_notional, tick_size

    try:
        exchange_info = client.futures_exchange_info()
        symbol_info = next(s for s in exchange_info['symbols'] if s['symbol'] == SYMBOL)

        for filter_info in symbol_info['filters']:
            if filter_info['filterType'] == 'LOT_SIZE':
                min_qty = float(filter_info['minQty'])
                max_qty = float(filter_info['maxQty'])
                step_size = float(filter_info['stepSize'])
            elif filter_info['filterType'] == 'MIN_NOTIONAL':
                min_notional = float(filter_info['notional'])
            elif filter_info['filterType'] == 'PRICE_FILTER':
                tick_size = float(filter_info['tickSize'])

        print(f"✅ Filtros de {SYMBOL}:")
        print(f"   Min cantidad: {min_qty}")
        print(f"   Step size: {step_size}")
        print(f"   Min notional: ${min_notional}")
        print(f"   Tick size: {tick_size}")

    except Exception as e:
        logging.error(f"Error obteniendo filtros: {e}")
        # Valores por defecto seguros para SOLUSDT Futures
        min_qty = 0.01
        step_size = 0.01
        min_notional = 2.0  # Futures permite desde $2 USDT
        tick_size = 0.001

def round_step_size(quantity, step_size):
    """Redondea la cantidad hacia abajo al múltiplo de step_size"""
    if step_size == 0:
        return quantity
    return math.floor(quantity / step_size) * step_size

def round_price(price, tick_size):
    """Redondea el precio al tick size correcto"""
    if tick_size == 0:
        return price
    return round(price / tick_size) * tick_size

def fetch_price_safely(retries=3):
    """MEJORA 3: Helper para obtener precio con reintentos"""
    for attempt in range(retries):
        try:
            ticker = client.futures_symbol_ticker(symbol=SYMBOL)
            return float(ticker['price'])
        except Exception as e:
            if attempt == retries - 1:
                raise e
            logging.warning(f"Error obteniendo precio (intento {attempt + 1}): {e}")
            time.sleep(1)
    return None

def calculate_ema(data, period):
    """Calcula EMA de una serie de precios"""
    if len(data) < period:
        return None

    df = pd.DataFrame(data)
    ema_series = df.ewm(span=period, adjust=False).mean()
    return float(ema_series.iloc[-1, 0])

def update_emas(klines_buffer):
    """Actualiza EMAs para un buffer de velas"""
    if len(klines_buffer) < EMA_LONG:
        return None, None, None

    closes = [float(k['close']) for k in klines_buffer]

    ema7 = calculate_ema(closes, EMA_SHORT)
    ema25 = calculate_ema(closes, EMA_MEDIUM) 
    ema99 = calculate_ema(closes, EMA_LONG)

    return ema7, ema25, ema99

def tendencia_actual(ema7, ema25, ema99):
    """Determina tendencia actual"""
    if ema7 is None or ema25 is None or ema99 is None:
        return None

    ema7_val = float(ema7) if hasattr(ema7, 'iloc') else float(ema7)
    ema25_val = float(ema25) if hasattr(ema25, 'iloc') else float(ema25)
    ema99_val = float(ema99) if hasattr(ema99, 'iloc') else float(ema99)

    if ema7_val > ema25_val > ema99_val:
        return "LONG"
    elif ema7_val < ema25_val < ema99_val:
        return "SHORT"
    else:
        return None

def confirmar_con_1m(signal_direction):
    """Confirma señal de 5m con 1m (EXACTAMENTE como tu backtest)"""
    if len(klines_1m) < 5:
        return False

    ultimas_5_velas = list(klines_1m)[-5:]
    current_1m = ultimas_5_velas[-1]
    
    if len(klines_1m) < EMA_LONG:
        return False
    
    ema7_1m, ema25_1m, ema99_1m = update_emas(klines_1m)
    if ema7_1m is None or ema25_1m is None or ema99_1m is None:
        return False

    if signal_direction == "LONG":
        return ema7_1m > ema25_1m > ema99_1m
    elif signal_direction == "SHORT":
        return ema7_1m < ema25_1m < ema99_1m
    
    return False

def check_trend_change():
    """NUEVO: Verifica si la tendencia cambió y debe cerrar posición"""
    if not position_open or len(klines_1m) < EMA_LONG:
        return False

    try:
        ema7_1m, ema25_1m, ema99_1m = update_emas(klines_1m)
        if ema7_1m is None or ema25_1m is None or ema99_1m is None:
            return False

        # Obtener las últimas 2 velas para confirmar cambio
        last_2_klines = list(klines_1m)[-2:]
        confirmaciones = 0
        
        for kline in last_2_klines:
            close_price = float(kline['close'])
            open_price = float(kline['open'])
            
            if position_side == SIDE_BUY:
                if (close_price < ema7_1m and
                    close_price < open_price and
                    ema7_1m < ema25_1m < ema99_1m):
                    confirmaciones += 1
                    
            elif position_side == SIDE_SELL:
                if (close_price > ema7_1m and
                    close_price > open_price and
                    ema7_1m > ema25_1m > ema99_1m):
                    confirmaciones += 1

        if confirmaciones >= 2:
            print(f"⚡ CAMBIO DE TENDENCIA CONFIRMADO: {confirmaciones}/2 velas")
            return True
        else:
            print(f"⏳ Cambio sin confirmar: {confirmaciones}/2 velas")
            return False

    except Exception as e:
        logging.error(f"Error verificando cambio de tendencia: {e}")
        return False

def apply_trailing_stop(current_price):
    """ARREGLADO: Trailing stop con inicialización correcta"""
    global trailing_sl_price
    
    if not TRAILING_STOP_ENABLED or not position_open:
        return False
        
    try:
        trailing_distance = current_price * TRAILING_DISTANCE_PCT
        
        if position_side == SIDE_BUY:
            # Para LONG: subir el SL si el precio sube
            new_trailing_sl = current_price - trailing_distance
            if trailing_sl_price == 0.0 or new_trailing_sl > trailing_sl_price:
                trailing_sl_price = round_price(new_trailing_sl, tick_size)
                print(f"🔄 Trailing SL actualizado (LONG): ${trailing_sl_price:.4f}")
                return True
                
        elif position_side == SIDE_SELL:
            # Para SHORT: bajar el SL si el precio baja
            new_trailing_sl = current_price + trailing_distance
            if trailing_sl_price == 0.0 or new_trailing_sl < trailing_sl_price:
                trailing_sl_price = round_price(new_trailing_sl, tick_size)
                print(f"🔄 Trailing SL actualizado (SHORT): ${trailing_sl_price:.4f}")
                return True
                
        return False
        
    except Exception as e:
        logging.error(f"Error aplicando trailing stop: {e}")
        return False

def check_entry_signals():
    """MEJORADO: Verifica señales con pendiente EMA99 y validación precio"""
    global position_open, current_position, entry_price, tp_price, sl_price, position_side, trades_today, entry_timestamp, trailing_sl_price, previous_ema99_5m
    
    if len(klines_5m) < EMA_LONG:
        return None

    try:
        last_candle = klines_5m[-1]
        close_price = float(last_candle['close'])
        open_price = float(last_candle['open'])

        ema7_5m, ema25_5m, ema99_5m = update_emas(klines_5m)
        if None in [ema7_5m, ema25_5m, ema99_5m]:
            return None

        # MEJORA 1: Calcular pendiente de EMA99
        if previous_ema99_5m is None:
            previous_ema99_5m = ema99_5m
            return None

        slope_ema99 = ema99_5m - previous_ema99_5m
        is_bear_slope = slope_ema99 < 0  # Pendiente bajista
        is_bull_slope = slope_ema99 > 0  # Pendiente alcista
        
        # Actualizar EMA99 anterior para próximo ciclo
        previous_ema99_5m = ema99_5m

        trend_5m = tendencia_actual(ema7_5m, ema25_5m, ema99_5m)
        if trend_5m is None:
            return None

        # Validar distancia a EMA25
        distancia = abs(close_price - ema25_5m) / ema25_5m
        if distancia > DISTANCIA_MAX:
            print(f"❌ Precio muy lejos de EMA25: {distancia*100:.2f}%")
            return None

        print(f"✅ Análisis 5m: Precio=${close_price:.2f} | EMA99={ema99_5m:.2f} | Pendiente={'📈' if is_bull_slope else '📉'} | Tendencia={trend_5m}")

        signal = None

        # MEJORA 2: Validaciones adicionales con precio vs EMA99 y pendiente
        if trend_5m == "LONG":
            # LONG: precio > EMA99, pendiente positiva, vela alcista
            if (close_price > ema99_5m and  # Precio sobre EMA99
                is_bull_slope and           # Pendiente alcista
                close_price > open_price and # Vela alcista
                close_price > ema7_5m):     # Confirmación adicional
                
                if confirmar_con_1m("LONG"):
                    signal = "LONG"
                    print("🚀 SEÑAL LONG REFINADA: precio>EMA99 + pendiente+ + vela alcista + conf.1m")
                else:
                    print("❌ LONG: Sin confirmación 1m")
            else:
                print(f"❌ LONG filtrado: precio>EMA99={close_price > ema99_5m} | pendiente+={is_bull_slope} | vela+={close_price > open_price}")

        elif trend_5m == "SHORT":
            # SHORT: precio < EMA99, pendiente negativa, vela bajista
            if (close_price < ema99_5m and  # Precio bajo EMA99
                is_bear_slope and           # Pendiente bajista
                close_price < open_price and # Vela bajista
                close_price < ema7_5m):     # Confirmación adicional
                
                if confirmar_con_1m("SHORT"):
                    signal = "SHORT"
                    print("🚀 SEÑAL SHORT REFINADA: precio<EMA99 + pendiente- + vela bajista + conf.1m")
                else:
                    print("❌ SHORT: Sin confirmación 1m")
            else:
                print(f"❌ SHORT filtrado: precio<EMA99={close_price < ema99_5m} | pendiente-={is_bear_slope} | vela-={close_price < open_price}")

        return signal
        
    except Exception as e:
        logging.error(f"Error verificando señales: {e}")
        return None

def setup_futures():
    """Configurar cuenta para futures"""
    try:
        get_symbol_info()

        current_leverage = None
        try:
            positions = client.futures_position_information(symbol=SYMBOL)
            if positions:
                current_leverage = int(positions[0]['leverage'])
        except Exception as e:
            logging.warning(f"No se pudo obtener leverage actual: {e}")

        if current_leverage != LEVERAGE:
            client.futures_change_leverage(symbol=SYMBOL, leverage=LEVERAGE)
            print(f"✅ Leverage configurado a {LEVERAGE}x para {SYMBOL}")
        else:
            print(f"✅ Leverage ya configurado a {LEVERAGE}x para {SYMBOL}")

        account = client.futures_account()
        balance = float(account['totalWalletBalance'])
        available_balance = float(account['availableBalance'])
        print(f"💰 Balance total: ${balance:.2f} USDT")
        print(f"💰 Balance disponible: ${available_balance:.2f} USDT")

        if available_balance < 2.0:
            print(f"⚠️ ADVERTENCIA: Balance muy bajo (${available_balance:.2f}) - recomendado mínimo: $2 USDT")
        else:
            # Calcular poder de compra con 90%
            margin_90_pct = available_balance * 0.90
            buying_power = margin_90_pct * LEVERAGE
            print(f"✅ Balance: ${available_balance:.2f} USDT")
            print(f"🎯 Usando 90%: ${margin_90_pct:.2f} USDT de margen")
            print(f"⚡ Poder de compra: ${buying_power:.2f} USDT (x{LEVERAGE} leverage)")

    except Exception as e:
        logging.error(f"Error configurando futures: {e}")

def validate_order_params(side, current_price):
    """ARREGLADO: Usa exactamente 90% del balance disponible como solicitas"""
    try:
        account = client.futures_account()
        available_balance = float(account['availableBalance'])

        print(f"💰 Balance total disponible: ${available_balance:.2f} USDT")

        # Verificar balance mínimo
        if available_balance < 2.0:
            return False, f"Balance insuficiente: ${available_balance:.2f} USDT - mínimo $2"

        # USAR EXACTAMENTE 90% DEL BALANCE COMO PEDISTE
        margin_to_use = available_balance * 0.90  # 90% como solicitas
        total_exposure = margin_to_use * LEVERAGE

        print(f"🎯 Usando 90% del balance: ${margin_to_use:.2f} USDT")
        print(f"⚡ Exposición total: ${total_exposure:.2f} USDT (x{LEVERAGE})")

        # Calcular cantidad SOL que puedes comprar
        quantity = total_exposure / current_price
        quantity = round_step_size(quantity, step_size)

        # Verificar límites del símbolo
        if quantity < min_qty:
            print(f"⚠️ Cantidad muy pequeña, usando mínimo: {min_qty}")
            quantity = min_qty

        if quantity > max_qty:
            print(f"⚠️ Cantidad muy grande, usando máximo: {max_qty}")
            quantity = max_qty

        # Recalcular valores finales con la cantidad ajustada
        final_notional = quantity * current_price
        final_margin_needed = final_notional / LEVERAGE

        print(f"📊 Cantidad final SOL: {quantity}")
        print(f"💵 Valor notional: ${final_notional:.2f} USDT")
        print(f"🔧 Margen necesario: ${final_margin_needed:.2f} USDT")
        print(f"💰 Balance después: ${available_balance - final_margin_needed:.2f} USDT")

        # Verificación final - debe ser suficiente
        if final_margin_needed > available_balance:
            # Si aún no alcanza, reducir un poco más
            safe_quantity = (available_balance * 0.85 * LEVERAGE) / current_price
            safe_quantity = round_step_size(safe_quantity, step_size)
            
            if safe_quantity >= min_qty:
                quantity = safe_quantity
                final_notional = quantity * current_price
                final_margin_needed = final_notional / LEVERAGE
                print(f"🔧 Ajustado a cantidad segura: {quantity} SOL")
            else:
                return False, f"Balance insuficiente para operar. Necesitas al menos ${(min_qty * current_price / LEVERAGE):.2f} USDT"

        return True, {
            'quantity': quantity,
            'notional': final_notional,
            'available_balance': available_balance,
            'required_margin': final_margin_needed
        }

    except Exception as e:
        logging.error(f"Error validando parámetros: {e}")
        return False, f"Error validando parámetros: {e}"

def place_market_order(side):
    """MEJORADO: Colocar orden con mejor manejo de errores"""
    global position_open, current_position, entry_price, tp_price, sl_price, position_side, trades_today, entry_timestamp, trailing_sl_price

    # Verificar límite diario (más conservador para cumplir normas)
    current_time = time.time()
    global last_day_reset
    if current_time - last_day_reset >= 86400:  # 24 horas
        trades_today = 0
        last_day_reset = current_time
        print(f"🔄 Límite diario reiniciado: 0/{MAX_TRADES_PER_DAY} trades")

    if trades_today >= MAX_TRADES_PER_DAY:
        print(f"⚠️ Límite de {MAX_TRADES_PER_DAY} trades/día alcanzado")
        return False

    try:
        # Obtener precio actual con reintentos
        ticker = None
        for attempt in range(3):
            try:
                ticker = client.futures_symbol_ticker(symbol=SYMBOL)
                break
            except Exception as e:
                if "rate limit" in str(e).lower():
                    print("⚠️ Rate limit detectado - esperando más tiempo...")
                    time.sleep(60)  # Esperar 1 minuto en caso de rate limit
                elif attempt == 2:
                    raise e
                else:
                    time.sleep(5)  # Esperar 5 segundos para otros errores
                
        current_price = float(ticker['price'])
        current_price = round_price(current_price, tick_size)

        # Validar parámetros
        is_valid, validation_result = validate_order_params(side, current_price)

        if not is_valid:
            print(f"❌ Validación fallida: {validation_result}")
            return False

        quantity = validation_result['quantity']
        notional = validation_result['notional']

        print(f"🔄 Ejecutando orden {side}")
        print(f"📊 Cantidad: {quantity} SOL")
        print(f"💵 Notional: ${notional:.2f} USDT")

        # Crear orden con reintentos
        order = None
        for attempt in range(3):
            try:
                order = client.futures_create_order(
                    symbol=SYMBOL,
                    side=side,
                    type=ORDER_TYPE_MARKET,
                    quantity=quantity
                )
                break
            except Exception as e:
                logging.error(f"Intento {attempt + 1} fallido: {e}")
                if attempt == 2:
                    raise e
                time.sleep(1)

        print(f"✅ Orden ejecutada: {order['orderId']}")

        trades_today += 1
        position_open = True
        position_side = side
        entry_price = current_price
        entry_timestamp = datetime.now()
        current_position = order

        # Calcular TP/SL
        if side == SIDE_BUY:
            tp_price = round_price(entry_price * (1 + TAKE_PROFIT_PCT), tick_size)
            sl_price = round_price(entry_price * (1 - STOP_LOSS_PCT), tick_size)
            trailing_sl_price = sl_price  # Inicializar trailing stop
        else:
            tp_price = round_price(entry_price * (1 - TAKE_PROFIT_PCT), tick_size)
            sl_price = round_price(entry_price * (1 + STOP_LOSS_PCT), tick_size)
            trailing_sl_price = sl_price  # Inicializar trailing stop

        print(f"🎯 Entry: ${entry_price:.4f} | TP: ${tp_price:.4f} | SL: ${sl_price:.4f}")
        
        # Nota: No colocamos SL/TP automáticos para tener control total
        
        return True

    except Exception as e:
        logging.error(f"Error colocando orden: {e}")
        return False

def close_position(reason="MANUAL"):
    """MEJORADO: Cerrar posición con mejor manejo de errores"""
    global position_open, current_position, position_side, trailing_sl_price

    if not position_open:
        return False

    try:
        positions = client.futures_position_information(symbol=SYMBOL)
        position = next((p for p in positions if float(p['positionAmt']) != 0), None)

        if not position:
            position_open = False
            trailing_sl_price = 0.0
            return True

        position_amt = float(position['positionAmt'])
        close_side = SIDE_SELL if position_amt > 0 else SIDE_BUY
        quantity = abs(position_amt)
        quantity = round_step_size(quantity, step_size)

        print(f"🔄 Cerrando posición ({reason}): {quantity} SOL")

        # Cerrar con reintentos
        order = None
        for attempt in range(3):
            try:
                order = client.futures_create_order(
                    symbol=SYMBOL,
                    side=close_side,
                    type=ORDER_TYPE_MARKET,
                    quantity=quantity
                )
                break
            except Exception as e:
                logging.error(f"Intento {attempt + 1} de cierre fallido: {e}")
                if attempt == 2:
                    raise e
                time.sleep(1)

        # Calcular PnL
        current_price = float(client.futures_symbol_ticker(symbol=SYMBOL)['price'])
        if position_amt > 0:  # Long
            pnl_pct = (current_price - entry_price) / entry_price * 100
        else:  # Short
            pnl_pct = (entry_price - current_price) / entry_price * 100

        print(f"✅ Posición cerrada | PnL: {pnl_pct:+.2f}% | Razón: {reason}")

        position_open = False
        current_position = None
        position_side = None
        trailing_sl_price = 0.0

        return True

    except Exception as e:
        logging.error(f"Error cerrando posición: {e}")
        return False

def monitor_position():
    """OPTIMIZADO: Monitorear posición con trailing stop y cambio de tendencia"""
    global position_open, position_side

    print("🔍 Iniciando monitoreo de posición...")

    while position_open:
        try:
            # Verificar cambio de tendencia primero
            if check_trend_change():
                print("⚡ Tendencia cambió - CIERRE POR TREND")
                close_position("TREND_CHANGE")
                break

            # Obtener precio actual con función helper
            current_price = fetch_price_safely()

            # Aplicar trailing stop
            if apply_trailing_stop(current_price):
                # Si se actualizó el trailing stop, usar ese como nuevo SL
                effective_sl = trailing_sl_price
            else:
                effective_sl = sl_price

            should_close = False
            reason = ""

            # Verificar condiciones de cierre
            if position_side == SIDE_BUY:
                if current_price >= tp_price:
                    should_close = True
                    reason = "TAKE_PROFIT"
                elif current_price <= effective_sl:
                    should_close = True
                    reason = "TRAILING_STOP" if effective_sl == trailing_sl_price else "STOP_LOSS"

            elif position_side == SIDE_SELL:
                if current_price <= tp_price:
                    should_close = True
                    reason = "TAKE_PROFIT"
                elif current_price >= effective_sl:
                    should_close = True
                    reason = "TRAILING_STOP" if effective_sl == trailing_sl_price else "STOP_LOSS"

            if should_close:
                close_position(reason)
                break

            print(f"📊 Monitor: ${current_price:.4f} | TP: ${tp_price:.4f} | SL: ${effective_sl:.4f}")
            time.sleep(10)  # Más conservador: 10 segundos para respetar límites

        except Exception as e:
            logging.error(f"Error en monitoreo: {e}")
            # En caso de error, esperar más tiempo para no sobrecargar la API
            time.sleep(30)

def validate_connection():
    """Validar conexión con Binance antes de iniciar"""
    try:
        client.ping()
        account = client.futures_account()
        print(f"✅ Conexión válida | Balance: ${float(account['totalWalletBalance']):.2f} USDT")

        exchange_info = client.futures_exchange_info()
        symbols = [s['symbol'] for s in exchange_info['symbols']]
        if SYMBOL not in symbols:
            raise ValueError(f"❌ {SYMBOL} no disponible en Futures")

        return True

    except Exception as e:
        logging.error(f"Error de conexión: {e}")
        return False

def on_message(ws, message):
    """MEJORADO: Procesar mensajes con validación robusta"""
    global last_5m_close_time, last_1m_close_time, position_open
    import json

    try:
        # Validar que el mensaje no esté vacío o corrupto
        if not message or len(message) < 10:
            return
            
        data = json.loads(message)

        # Validar estructura del mensaje
        if 'stream' not in data or 'data' not in data:
            return
            
        if 'k' not in data['data']:
            return

        stream = data['stream']
        kline_data = data['data']['k']

        # Validar que tiene todos los campos necesarios
        required_fields = ['t', 'T', 'o', 'h', 'l', 'c', 'v', 'x']
        if not all(field in kline_data for field in required_fields):
            print("⚠️ Datos de vela incompletos - ignorando...")
            return

        # Solo procesar velas cerradas
        if not kline_data['x']:  # x = is_closed
            return

        kline = {
            'open_time': kline_data['t'],
            'close_time': kline_data['T'],
            'open': kline_data['o'],
            'high': kline_data['h'],
            'low': kline_data['l'],
            'close': kline_data['c'],
            'volume': kline_data['v']
        }

        if '1m' in stream:
            klines_1m.append(kline)
            last_1m_close_time = kline['close_time']
            print(f"📊 1m: ${float(kline['close']):.2f}")

        elif '5m' in stream:
            klines_5m.append(kline)
            last_5m_close_time = kline['close_time']
            print(f"📈 5m: ${float(kline['close']):.2f}")

            # ARREGLADO: Verificar señales con protección contra duplicadas
            if not position_open:
                # Verificar que no hay posición real en Binance
                try:
                    positions = client.futures_position_information(symbol=SYMBOL)
                    real_position = next((p for p in positions if float(p['positionAmt']) != 0), None)
                    
                    if real_position:
                        print("⚠️ Posición detectada en Binance - sincronizando...")
                        position_open = True
                        return
                except Exception as e:
                    logging.error(f"Error verificando posición: {e}")
                
                signal = check_entry_signals()

                if signal == "LONG":
                    print("🟢 EJECUTANDO LONG")
                    if place_market_order(SIDE_BUY):
                        monitor_thread = threading.Thread(target=monitor_position)
                        monitor_thread.daemon = True
                        monitor_thread.start()

                elif signal == "SHORT":
                    print("🔴 EJECUTANDO SHORT")
                    if place_market_order(SIDE_SELL):
                        monitor_thread = threading.Thread(target=monitor_position)
                        monitor_thread.daemon = True
                        monitor_thread.start()
                        
                else:
                    print("⏳ Sin señales claras - esperando...")

    except Exception as e:
        logging.error(f"Error procesando mensaje: {e}")

def on_error(ws, error):
    global websocket_connected
    websocket_connected = False
    logging.error(f"❌ WebSocket error: {error}")
    print("🔄 Intentando reconectar...")

def on_close(ws, close_status_code, close_msg):
    global websocket_connected
    websocket_connected = False
    print(f"🔌 WebSocket desconectado: {close_status_code} - {close_msg}")
    print("🔄 Reconectando automáticamente...")

def on_open(ws):
    global websocket_connected, reconnection_attempts
    websocket_connected = True
    reconnection_attempts = 0
    print("✅ WebSocket conectado - Recibiendo datos en tiempo real")
    print("📡 Monitoreando SOLUSDT 1m y 5m...")

def start_websocket():
    """MEJORADO: Iniciar WebSocket con mejor gestión"""
    streams = [
        f"{SYMBOL.lower()}@kline_1m",
        f"{SYMBOL.lower()}@kline_5m"
    ]

    socket_url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

    print(f"🔌 Conectando WebSocket...")
    print(f"📡 Streams: {streams}")

    ws = WebSocketApp(
        socket_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )

    ws.run_forever()

def start_websocket_with_retry():
    """ARREGLADO: Reconexión inteligente que nunca se cuelga"""
    global reconnection_attempts, websocket_connected
    
    while reconnection_attempts < max_reconnection_attempts:
        try:
            if reconnection_attempts > 0:
                # Backoff muy conservador para cumplir normas: 60s, 120s, 300s, 600s, máximo 1200s (20min)
                wait_time = min(60 * (2 ** (reconnection_attempts - 1)), 1200)
                print(f"🔄 Reintentando conexión en {wait_time} segundos (normas Binance)...")
                time.sleep(wait_time)
                
            reconnection_attempts += 1
            print(f"📡 Intento de conexión #{reconnection_attempts}/{max_reconnection_attempts}")
            
            # Verificar conectividad antes de intentar WebSocket
            try:
                client.ping()
                print("✅ Conectividad con Binance OK")
            except Exception as ping_error:
                print(f"❌ Sin conectividad con Binance: {ping_error}")
                continue
            
            start_websocket()
            
        except KeyboardInterrupt:
            print("\n🛑 Bot detenido por el usuario")
            break
        except Exception as e:
            logging.error(f"Error en WebSocket (intento {reconnection_attempts}): {e}")
            
            if reconnection_attempts >= max_reconnection_attempts:
                print("🛑 Máximo de reintentos alcanzado. Pausa larga para respetar normas...")
                reconnection_attempts = 0  # Reiniciar para intentar indefinidamente
                time.sleep(600)  # 10 minutos de pausa para ser respetuoso con Binance
                
            websocket_connected = False

def initialize_historical_data():
    """MEJORADO: Cargar datos históricos con mejor manejo de errores"""
    try:
        print("📊 Inicializando EMAs con datos históricos...")

        # Cargar datos con reintentos
        for attempt in range(3):
            try:
                # 1m histórico
                klines_1m_hist = client.futures_klines(symbol=SYMBOL, interval='1m', limit=200)
                for kline in klines_1m_hist:
                    kline_dict = {
                        'open_time': kline[0],
                        'close_time': kline[6],
                        'open': kline[1],
                        'high': kline[2],
                        'low': kline[3],
                        'close': kline[4],
                        'volume': kline[5]
                    }
                    klines_1m.append(kline_dict)

                # 5m histórico
                klines_5m_hist = client.futures_klines(symbol=SYMBOL, interval='5m', limit=200)
                for kline in klines_5m_hist:
                    kline_dict = {
                        'open_time': kline[0],
                        'close_time': kline[6],
                        'open': kline[1],
                        'high': kline[2],
                        'low': kline[3],
                        'close': kline[4],
                        'volume': kline[5]
                    }
                    klines_5m.append(kline_dict)
                break
                
            except Exception as e:
                if attempt == 2:
                    raise e
                logging.warning(f"Reintentando cargar datos históricos (intento {attempt + 1}): {e}")
                time.sleep(2)

        print(f"✅ EMAs inicializadas: {len(klines_1m)} velas 1m, {len(klines_5m)} velas 5m")
        print("🔄 Cambiando a datos WebSocket en tiempo real...")

    except Exception as e:
        logging.error(f"Error cargando datos históricos: {e}")
        raise e

def main():
    print("🚀 BOT DE TRADING MULTI-TIMEFRAME REFINADO")
    print("="*60)
    print(f"📈 Symbol: {SYMBOL}")
    print(f"⚡ Leverage: {LEVERAGE}x")
    print(f"💰 Capital por trade: 90% del balance disponible")
    print(f"🎯 TP/SL: {TAKE_PROFIT_PCT*100:.1f}%")
    print(f"🔄 Trailing Stop: {'✅ Activado' if TRAILING_STOP_ENABLED else '❌ Desactivado'}")
    print(f"🛡️ Máximo: {MAX_TRADES_PER_DAY} trades/día")
    print(f"💰 Notional mínimo: ${min_notional} (Futures)")
    print("📊 MEJORAS IMPLEMENTADAS:")
    print("   ✅ Pendiente de EMA99 (filtro anti-señales falsas)")
    print("   ✅ Validación precio vs EMA99 (tendencias sólidas)")
    print("   ✅ Código optimizado (helpers y limpieza)")
    print("="*60)

    # Validar conexión
    if not validate_connection():
        print("🛑 No se pudo establecer conexión con Binance")
        return

    # Configurar futures
    setup_futures()

    # Cargar datos históricos
    initialize_historical_data()

    try:
        # Iniciar WebSocket con reconexión automática
        start_websocket_with_retry()
    except KeyboardInterrupt:
        print("\n🛑 Bot detenido por el usuario")
        if position_open:
            print("🔄 Cerrando posición abierta...")
            close_position("USER_STOP")

if __name__ == "__main__":
    main()
