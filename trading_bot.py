import os
import time
import json
import websocket
import threading
import logging
from datetime import datetime
from binance.client import Client
from binance.enums import *
import math
import pandas as pd
from collections import deque

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# CONFIGURACIÓN DE BINANCE
API_KEY = os.environ.get('BINANCE_API_KEY')
SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY')

if not API_KEY or not SECRET_KEY:
    raise ValueError("❌ ERROR: Configura BINANCE_API_KEY y BINANCE_SECRET_KEY en Secrets")

client = Client(API_KEY, SECRET_KEY)

# CONFIGURACIÓN DE TRADING (Valores rentables de backtest_optimizado.py)
SYMBOL = "SOLUSDT"
LEVERAGE = 5                # 5x - Conservador y rentable
TAKE_PROFIT_PCT = 0.01      # 1.0% - Rentable comprobado
STOP_LOSS_PCT = 0.0075      # 0.75% - Rentable comprobado
DISTANCIA_MAX = 0.02        # 2% máximo distancia a EMA25

# EMAS (Valores rentables)
EMA_SHORT = 7
EMA_MEDIUM = 25
EMA_LONG = 99

# PROTECCIONES
MAX_TRADES_DAY = 5          # Máximo 5 trades/día
MAX_LOSS_DAY = 50.0         # Máximo $50 pérdida/día
MIN_BALANCE = 10.0          # Balance mínimo para operar
MIN_NOTIONAL = 2            # Mínimo $2 USDT por orden

# Variables globales
klines_1m = deque(maxlen=200)
klines_5m = deque(maxlen=200)
position_open = False
entry_price = 0.0
position_side = None
trades_today = 0
loss_today = 0.0

def round_step_size(quantity, step_size):
    """Redondear cantidad según step_size"""
    return math.floor(quantity / step_size) * step_size

def calculate_ema(closes, period):
    """Calcular EMA usando pandas"""
    if len(closes) < period:
        return None
    df = pd.Series(closes)
    return float(df.ewm(span=period, adjust=False).mean().iloc[-1])

def get_emas(klines_buffer):
    """Obtener EMAs de un buffer de klines"""
    if len(klines_buffer) < EMA_LONG:
        return None, None, None

    closes = [float(k['close']) for k in klines_buffer]
    ema7 = calculate_ema(closes, EMA_SHORT)
    ema25 = calculate_ema(closes, EMA_MEDIUM)
    ema99 = calculate_ema(closes, EMA_LONG)

    return ema7, ema25, ema99

def get_trend(ema7, ema25, ema99):
    """Determinar tendencia actual"""
    if None in [ema7, ema25, ema99]:
        return None

    if ema7 > ema25 > ema99:
        return "LONG"
    elif ema7 < ema25 < ema99:
        return "SHORT"
    else:
        return None

def confirm_with_1m(signal_direction):
    """Confirmar señal 5m con tendencia 1m"""
    ema7_1m, ema25_1m, ema99_1m = get_emas(klines_1m)
    if None in [ema7_1m, ema25_1m, ema99_1m]:
        return False

    trend_1m = get_trend(ema7_1m, ema25_1m, ema99_1m)
    return trend_1m == signal_direction

def check_early_exit():
    """Verificar si 1m cambió de tendencia"""
    if not position_open:
        return False

    ema7_1m, ema25_1m, ema99_1m = get_emas(klines_1m)
    if None in [ema7_1m, ema25_1m, ema99_1m]:
        return False

    trend_1m = get_trend(ema7_1m, ema25_1m, ema99_1m)

    # Si 1m cambió opuesta a nuestra posición
    if position_side == SIDE_BUY and trend_1m == "SHORT":
        return True
    elif position_side == SIDE_SELL and trend_1m == "LONG":
        return True

    return False

def check_signals():
    """Verificar señales de entrada (lógica de backtest_optimizado.py)"""
    if len(klines_5m) < EMA_LONG:
        return None

    # Obtener datos actuales
    last_candle = klines_5m[-1]
    close_price = float(last_candle['close'])
    open_price = float(last_candle['open'])

    # EMAs de 5m
    ema7_5m, ema25_5m, ema99_5m = get_emas(klines_5m)
    if None in [ema7_5m, ema25_5m, ema99_5m]:
        return None

    # Determinar tendencia 5m
    trend_5m = get_trend(ema7_5m, ema25_5m, ema99_5m)
    if trend_5m is None:
        return None

    # Validar distancia a EMA25
    distance = abs(close_price - ema25_5m) / ema25_5m
    if distance > DISTANCIA_MAX:
        return None

    # Lógica de entrada (MISMA de backtest rentable)
    signal = None

    if trend_5m == "LONG":
        # LONG: precio > EMA7 + vela verde
        if close_price > ema7_5m and close_price > open_price:
            if confirm_with_1m("LONG"):
                signal = "LONG"
                logging.info(f"✅ SEÑAL LONG: 5m alcista + confirmación 1m")

    elif trend_5m == "SHORT":
        # SHORT: precio < EMA7 + vela roja
        if close_price < ema7_5m and close_price < open_price:
            if confirm_with_1m("SHORT"):
                signal = "SHORT"
                logging.info(f"✅ SEÑAL SHORT: 5m bajista + confirmación 1m")

    return signal

def validate_order():
    """Validar si podemos hacer una orden"""
    global trades_today, loss_today

    # Reset diario
    current_time = time.time()
    hour = datetime.fromtimestamp(current_time).hour
    if hour == 0 and trades_today > 0:  # Reset a medianoche
        trades_today = 0
        loss_today = 0.0
        logging.info("🔄 Reset diario - Contadores reiniciados")

    # Verificar límites
    if trades_today >= MAX_TRADES_DAY:
        logging.warning(f"🛑 Límite trades/día: {trades_today}/{MAX_TRADES_DAY}")
        return False

    if loss_today >= MAX_LOSS_DAY:
        logging.warning(f"🛑 Límite pérdida/día: ${loss_today:.2f}")
        return False

    # Verificar balance
    try:
        account = client.futures_account()
        balance = float(account['availableBalance'])

        if balance < MIN_BALANCE:
            logging.warning(f"🛑 Balance muy bajo: ${balance:.2f}")
            return False

        if balance < MIN_NOTIONAL:
            logging.warning(f"🛑 Balance insuficiente para orden mínima")
            return False

        return True

    except Exception as e:
        logging.error(f"❌ Error verificando balance: {e}")
        return False

def place_order(signal):
    """Ejecutar orden de mercado"""
    global position_open, entry_price, position_side, trades_today

    try:
        # Obtener balance y calcular cantidad
        account = client.futures_account()
        balance = float(account['availableBalance'])

        # Usar 100% del balance (como en backtest)
        position_size = balance * 1.0

        # Precio actual y cantidad
        ticker = client.futures_symbol_ticker(symbol=SYMBOL)
        current_price = float(ticker['price'])
        quantity = position_size / current_price
        quantity = round_step_size(quantity, 0.01)

        # Determinar side
        side = SIDE_BUY if signal == "LONG" else SIDE_SELL

        logging.info(f"🔄 Ejecutando {signal}: {quantity} SOL @ ${current_price:.2f}")

        # Ejecutar orden
        order = client.futures_create_order(
            symbol=SYMBOL,
            side=side,
            type=ORDER_TYPE_MARKET,
            quantity=quantity
        )

        # Actualizar estado
        position_open = True
        position_side = side
        entry_price = current_price
        trades_today += 1

        logging.info(f"✅ Orden ejecutada: ID {order['orderId']}")
        return True

    except Exception as e:
        logging.error(f"❌ Error ejecutando orden: {e}")
        return False

def close_position(reason="MANUAL"):
    """Cerrar posición actual"""
    global position_open, position_side, loss_today

    if not position_open:
        return

    try:
        # Obtener posición actual
        positions = client.futures_position_information(symbol=SYMBOL)
        position = next((p for p in positions if float(p['positionAmt']) != 0), None)

        if not position:
            position_open = False
            return

        position_amt = float(position['positionAmt'])
        close_side = SIDE_SELL if position_amt > 0 else SIDE_BUY
        quantity = abs(position_amt)

        # Cerrar posición
        order = client.futures_create_order(
            symbol=SYMBOL,
            side=close_side,
            type=ORDER_TYPE_MARKET,
            quantity=quantity
        )

        # Calcular PnL
        current_price = float(client.futures_symbol_ticker(symbol=SYMBOL)['price'])
        if position_amt > 0:  # Long
            pnl_pct = (current_price - entry_price) / entry_price
        else:  # Short
            pnl_pct = (entry_price - current_price) / entry_price

        pnl_usd = pnl_pct * abs(position_amt) * entry_price

        # Actualizar pérdidas diarias
        if pnl_usd < 0:
            loss_today += abs(pnl_usd)

        logging.info(f"✅ Posición cerrada: PnL {pnl_pct*100:+.2f}% (${pnl_usd:+.2f}) - {reason}")

        position_open = False
        position_side = None

    except Exception as e:
        logging.error(f"❌ Error cerrando posición: {e}")

def monitor_position():
    """Monitor posición activa para TP/SL y cierre temprano"""
    logging.info("🔍 Iniciando monitoreo de posición...")

    while position_open:
        try:
            # Verificar cierre temprano por 1m
            if check_early_exit():
                logging.info("⚡ 1m cambió de tendencia - CERRANDO")
                close_position("1M_BREAK")
                break

            # Verificar TP/SL
            current_price = float(client.futures_symbol_ticker(symbol=SYMBOL)['price'])

            if position_side == SIDE_BUY:  # Long
                tp_price = entry_price * (1 + TAKE_PROFIT_PCT)
                sl_price = entry_price * (1 - STOP_LOSS_PCT)

                if current_price >= tp_price:
                    close_position("TAKE_PROFIT")
                    break
                elif current_price <= sl_price:
                    close_position("STOP_LOSS")
                    break

            elif position_side == SIDE_SELL:  # Short
                tp_price = entry_price * (1 - TAKE_PROFIT_PCT)
                sl_price = entry_price * (1 + STOP_LOSS_PCT)

                if current_price <= tp_price:
                    close_position("TAKE_PROFIT")
                    break
                elif current_price >= sl_price:
                    close_position("STOP_LOSS")
                    break

            time.sleep(3)  # Check cada 3 segundos

        except Exception as e:
            logging.error(f"❌ Error en monitoreo: {e}")
            time.sleep(5)

def on_message(ws, message):
    """Procesar mensajes WebSocket"""
    try:
        data = json.loads(message)

        if 'stream' not in data:
            return

        kline_data = data['data']['k']

        # Solo procesar velas cerradas
        if not kline_data['x']:
            return

        kline = {
            'open': kline_data['o'],
            'high': kline_data['h'],
            'low': kline_data['l'],
            'close': kline_data['c'],
            'volume': kline_data['v']
        }

        stream = data['stream']

        if '1m' in stream:
            klines_1m.append(kline)

        elif '5m' in stream:
            klines_5m.append(kline)
            logging.info(f"📊 5m: ${float(kline['close']):.2f}")

            # Verificar señales solo en cierre de 5m
            if not position_open and validate_order():
                signal = check_signals()

                if signal:
                    if place_order(signal):
                        # Iniciar monitoreo en thread separado
                        monitor_thread = threading.Thread(target=monitor_position)
                        monitor_thread.daemon = True
                        monitor_thread.start()

    except Exception as e:
        logging.error(f"❌ Error procesando mensaje: {e}")

def on_error(ws, error):
    logging.error(f"❌ WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    logging.warning(f"🔌 WebSocket cerrado: {close_status_code}")
    time.sleep(5)
    start_websocket()

def on_open(ws):
    logging.info("✅ WebSocket conectado - Datos en tiempo real activados")

def start_websocket():
    """Iniciar WebSocket para datos 1m y 5m"""
    streams = [
        f"{SYMBOL.lower()}@kline_1m",
        f"{SYMBOL.lower()}@kline_5m"
    ]

    url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

    ws = websocket.WebSocketApp(
        url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )

    ws.run_forever()

def setup_futures():
    """Configurar cuenta futures"""
    try:
        client.futures_change_leverage(symbol=SYMBOL, leverage=LEVERAGE)

        account = client.futures_account()
        balance = float(account['totalWalletBalance'])

        logging.info(f"✅ Futures configurado: {LEVERAGE}x leverage")
        logging.info(f"💰 Balance: ${balance:.2f} USDT")

        return balance >= MIN_BALANCE

    except Exception as e:
        logging.error(f"❌ Error configurando futures: {e}")
        return False

def load_initial_data():
    """Cargar datos históricos para inicializar EMAs"""
    try:
        logging.info("📊 Cargando datos iniciales...")

        # Cargar 200 velas históricas (solo una vez)
        klines_1m_hist = client.futures_klines(symbol=SYMBOL, interval='1m', limit=200)
        for kline in klines_1m_hist:
            klines_1m.append({
                'open': kline[1],
                'high': kline[2],
                'low': kline[3],
                'close': kline[4],
                'volume': kline[5]
            })

        klines_5m_hist = client.futures_klines(symbol=SYMBOL, interval='5m', limit=200)
        for kline in klines_5m_hist:
            klines_5m.append({
                'open': kline[1],
                'high': kline[2],
                'low': kline[3],
                'close': kline[4],
                'volume': kline[5]
            })

        logging.info(f"✅ Datos inicializados: {len(klines_1m)} velas 1m, {len(klines_5m)} velas 5m")
        return True

    except Exception as e:
        logging.error(f"❌ Error cargando datos: {e}")
        return False

def main():
    """Función principal del bot"""
    print("🚀 BOT DE TRADING LIMPIO - LÓGICA RENTABLE")
    print("="*50)
    print(f"📈 Symbol: {SYMBOL}")
    print(f"⚡ Leverage: {LEVERAGE}x")
    print(f"🎯 TP: {TAKE_PROFIT_PCT*100}% | SL: {STOP_LOSS_PCT*100}%")
    print(f"🔄 Estrategia: EMAs {EMA_SHORT}/{EMA_MEDIUM}/{EMA_LONG}")
    print(f"🛡️ Límites: {MAX_TRADES_DAY} trades/día, ${MAX_LOSS_DAY} pérdida máx")
    print("="*50)

    # Validar conexión
    try:
        client.ping()
        logging.info("✅ Conexión con Binance verificada")
    except Exception as e:
        logging.error(f"❌ Error conectando con Binance: {e}")
        return

    # Configurar futures
    if not setup_futures():
        logging.error("❌ Error configurando futures - Deteniendo bot")
        return

    # Cargar datos iniciales
    if not load_initial_data():
        logging.error("❌ Error cargando datos iniciales - Deteniendo bot")
        return

    # Iniciar WebSocket
    try:
        logging.info("🔄 Iniciando WebSocket...")
        start_websocket()

    except KeyboardInterrupt:
        logging.info("🛑 Bot detenido por usuario")
        if position_open:
            close_position("USER_STOP")

if __name__ == "__main__":
    main()