import ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import logging
from typing import Dict, List, Tuple, Optional
from firebase_config import firebase_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AdaptiveTradingPod:
    """Volatility-adaptive cryptocurrency trading module"""
    
    def __init__(self, exchange_id: str = 'binance'):
        """
        Initialize trading pod with exchange connection
        
        Args:
            exchange_id: Exchange identifier (binance, coinbasepro, etc.)
        """
        self.exchange_id = exchange_id
        self.exchange = None
        self.symbols = ['MATIC/USDT', 'ALGO/USDT', 'AAVE/USDT']
        self.position_sizes = {}  # Track active positions
        self.initialize_exchange()
        
    def initialize_exchange(self):
        """Initialize exchange connection with API keys from Firestore"""
        try:
            state = firebase_manager.get_mission_state()
            
            # In production, API keys would be encrypted in Firestore
            # For now, we'll use environment variables for security
            import os
            api_key = os.getenv('BINANCE_API_KEY')
            api_secret = os.getenv('BINANCE_API_SECRET')
            
            if not api_key or not api_secret:
                logger.error("Exchange API keys not found in environment")
                firebase_manager.update_field(
                    "mission_state", "current", 
                    "system_health.trading_paused", True
                )
                return
            
            exchange_class = getattr(ccxt, self.exchange_id)
            self.exchange = exchange_class({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'spot',
                    'adjustForTimeDifference': True
                }
            })
            
            # Test connection
            self.exchange.fetch_status()
            logger.info(f"✅ Connected to {self.exchange_id}")
            
        except ccxt.AuthenticationError as e:
            logger.error(f"Authentication failed: {e}")
            firebase_manager.update_field(
                "mission_state", "current",
                "system_health.last_error", "Exchange auth failed"
            )
        except Exception as e:
            logger.error(f"Exchange initialization failed: {e}")
    
    def calculate_dynamic_take_profit(self, volatility_24h: float, win_streak: int) -> float:
        """
        Calculate dynamic take profit percentage based on market conditions
        
        Args:
            volatility_24h: 24-hour volatility percentage
            win_streak: Number of consecutive winning trades
            
        Returns:
            Take profit percentage (e.g., 0.015 for 1.5%)
        """
        try:
            base = 0.008  # 0.8% base profit target
            volatility_factor = min(volatility_24h * 0.5, 0.015)  # Max 1.5%
            streak_bonus = min(win_streak * 0.002, 0.006)  # Max 0.6% bonus
            
            # Circuit breaker: reduce target during losses
            state = firebase_manager.get_mission_state()
            if state["system_health"]["consecutive_losses"] >= 2:
                base = 0.005  # Reduce to 0.5% during losing streak
            
            take_profit = base + volatility_factor + streak_bonus
            return min(take_profit, 0.03)  # Absolute max 3%
            
        except Exception as e:
            logger.error(f"Failed to calculate take profit: {e}")
            return 0.01  # Default 1% fallback
    
    def calculate_position_size(self, symbol: str, risk_percent: float = 0.02) -> float:
        """
        Calculate position size based on available capital and risk
        
        Args:
            symbol: Trading pair symbol
            risk_percent: Maximum risk per trade (default 2%)
            
        Returns:
            Position size in quote currency
        """
        try:
            state = firebase_manager.get_mission_state()
            available_capital = state["current_allocation"]["trading_capital"]
            
            # Apply circuit breakers
            health = state["system_health"]
            if health.get("trading_paused", False):
                return 0.0
            
            if health["consecutive_losses"] >= 3:
                risk_percent *= 0.5  # Reduce position size by 50%
            
            # Get current price
            ticker = self.exchange.fetch_ticker(symbol)
            current_price = ticker['last']
            
            position_size = (available_capital * risk_percent) / current_price
            return min(position_size, available_capital * 0.1)  # Max 10% of capital
            
        except Exception as e:
            logger.error(f"Position size calculation failed: {e}")
            return 0.0
    
    def analyze_market(self, symbol: str, timeframe: str = '5m') -> Dict:
        """
        Perform technical analysis on symbol
        
        Returns:
            Dictionary with analysis results
        """
        try:
            # Fetch OHLCV data
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # Calculate indicators
            df['returns'] = df['close'].pct_change()
            volatility = df['returns'].std() * np.sqrt(252 * 288)  # Annualized
            rsi = self._calculate_rsi(df['close'])
            volume_avg = df['volume'].rolling(20).mean().iloc[-1]
            
            # Entry signal logic (simplified mean reversion)
            current_rsi = rsi.iloc[-1]
            entry_signal = False
            
            if current_rsi < 35:  # Oversold
                entry_signal = True
            elif current_rsi > 65:  # Overbought (for short trades)
                # In spot markets, we only go long for simplicity
                entry_signal = False
            
            return {
                'symbol': symbol,
                'current_price': df['close'].iloc[-1],
                'volatility_24h': volatility,
                'rsi': current_rsi,
                'volume_trend': df['volume'].iloc[-1] > volume_avg * 1.2,
                'entry_signal': entry_signal,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Market analysis failed for {symbol}: {e}")
            return {'error': str(e)}
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate RSI indicator"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def execute_trade_cycle(self):
        """Main trading cycle execution"""
        try:
            logger.info("🔁 Starting trade cycle")
            
            # Check system health
            state = firebase_manager.get_mission_state()
            if state["system_health"].get("trading_paused", False):
                logger.info("Trading paused by system health check")
                return
            
            # Calculate win streak from recent trades
            win_streak = self._calculate_win_streak()
            
            # Analyze each symbol
            for symbol in self.symbols:
                analysis = self.analyze_market(symbol)
                
                if analysis.get('error'):
                    continue
                
                if analysis['entry_signal']:
                    # Calculate dynamic parameters
                    take_profit_pct = self.calculate_dynamic_take_profit(
                        analysis['volatility_24h'], win_streak
                    )
                    stop_loss_pct = take_profit_pct * 0.66
                    
                    # Calculate position size
                    position_size = self.calculate_position_size(symbol)
                    
                    if position_size > 0:
                        self._place_trade(
                            symbol=symbol,
                            amount=position_size,
                            take_profit_pct=take_profit_pct,
                            stop_loss_pct=stop_loss_pct,
                            analysis=analysis
                        )
                        time.sleep(1)  # Rate limiting
            
            # Update health timestamp
            firebase_manager.update_field(
                "mission_state", "current",
                "system_health.last_trade_check", datetime.now().isoformat()
            )
            
        except Exception as e:
            logger.error(f"Trade cycle failed: {e}")
            firebase_manager.update_field(
                "mission_state", "current",
                "system_health.last_error", f"Trade cycle: {str(e)[:100]}"
            )
    
    def _calculate_win_streak(self) -> int:
        """Calculate current win streak from Firestore trades"""
        try:
            # This would query Firestore for recent trades
            # For now, return 0 as placeholder
            return 0
        except:
            return 0
    
    def _place_trade(self, symbol: str, amount: float, 
                    take_profit_pct: float, stop_loss_pct: float, analysis: Dict):
        """Execute trade with proper risk management"""
        try:
            logger.info(f"📈 Placing trade: {symbol}, Amount: {amount:.4f}")
            
            # In production, this would place actual orders
            # For simulation/testing, we'll log to Firestore
            trade_data = {
                'symbol': symbol,
                'amount': amount,
                'type': 'LONG',
                'take_profit_pct': take_profit_pct,
                'stop_loss_pct': stop_loss_pct,
                'analysis': analysis,
                'status': 'simulated',  # Change to 'executed' for live trading
                'expected_profit_usd': amount * analysis['current_price'] * take_pro