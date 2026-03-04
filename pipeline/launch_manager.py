import json
import time
import random
import threading
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import requests
from solders.keypair import Keypair
from solders.pubkey import Pubkey
import base58
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

from utils.logger import setup_logger
from pipeline.control import load_control

console = Console()
logger = setup_logger("INFO")

WALLETS_PATH = Path("data/wallets.json")
LAST_MINT_PATH = Path("data/last_mint.txt")
HISTORY_PATH = Path("data/launch_history.json")

EXECUTOR_URL = "http://127.0.0.1:8790"
RPC_URL = "https://mainnet.helius-rpc.com/?api-key=fcf65e5f-636e-4800-ba08-33f6d536bace"

class LaunchManager:
    def __init__(self):
        self.control = load_control()
        self.wallets: List[Dict] = self._load_wallets()
        self.main_kp = self._load_main_keypair()
        self.auto_sell_running = False
        self.auto_sell_thread = None        
        self.tp_percent = None
        self.trailing_percent = None
        self.launch_running = False
        self.volume_running = False
        self.launch_thread = None
        self.volume_thread = None
        self.volume_logs: List[str] = []
        self.volume_start_time = None
        self.volume_minutes = 0
        self.wallet_positions = {}

    def _load_main_keypair(self):
        paths = [
            Path("id.json"),
            Path("executor_ts/id.json"),
            Path(__file__).parent.parent / "id.json",
        ]
        for p in paths:
            if p.exists():
                try:
                    with open(p, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    kp = Keypair.from_bytes(bytes(data))
                    logger.success(f"✅ Главный ключ загружен: {kp.pubkey()}")
                    return kp
                except:
                    continue
        logger.error("❌ id.json не найден!")
        return None

    def _load_wallets(self) -> List[Dict]:
        if WALLETS_PATH.exists():
            return json.loads(WALLETS_PATH.read_text(encoding="utf-8"))
        return []

    def _save_wallets(self):
        WALLETS_PATH.parent.mkdir(exist_ok=True)
        WALLETS_PATH.write_text(json.dumps(self.wallets, indent=2, ensure_ascii=False))

    def _save_last_mint(self, mint: str):
        LAST_MINT_PATH.parent.mkdir(exist_ok=True)
        LAST_MINT_PATH.write_text(mint)

    def _load_last_mint(self) -> str:
        if LAST_MINT_PATH.exists():
            return LAST_MINT_PATH.read_text().strip()
        return ""

    def _save_launch_history(self, data: dict):
        HISTORY_PATH.parent.mkdir(exist_ok=True)
        if HISTORY_PATH.exists():
            history = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        else:
            history = []
        history.append(data)
        HISTORY_PATH.write_text(json.dumps(history, indent=2, ensure_ascii=False))

    # ====================== GENERATE ======================
    def generate_wallets(self, num: int = 15, force: bool = False):
        if self.wallets and not force:
            logger.warning("Уже есть кошельки. Используй --force")
            return
        logger.info(f"Генерирую {num} кошельков...")
        self.wallets.clear()
        for i in range(num):
            kp = Keypair()
            secret_b58 = base58.b58encode(bytes(kp.secret())).decode("utf-8")
            self.wallets.append({
                "index": i,
                "pubkey": str(kp.pubkey()),
                "secret_b58": secret_b58
            })
        self._save_wallets()
        logger.success(f"✅ {num} кошельков сохранены")

    # ====================== FUND ALL ======================
    def fund_all(self, sol_amount: float):
        if not self.main_kp:
            console.print("[red]Главный ключ не найден![/red]")
            return
        console.print(Panel.fit(
            f"[bold]Отправляем {sol_amount} SOL на каждый из {len(self.wallets)} кошельков[/bold]",
            title="Fund All Wallets",
            border_style="green"
        ))
        for w in self.wallets:
            try:
                payload = {"side": "transfer", "to": w["pubkey"], "amount": sol_amount, "dry_run": False}
                r = requests.post(f"{EXECUTOR_URL}/trade", json=payload, timeout=30)
                if r.status_code == 200:
                    console.print(f"  → {w['pubkey'][:8]}... [green]OK[/green]")
                else:
                    console.print(f"  → {w['pubkey'][:8]}... [red]ошибка[/red]")
            except:
                console.print(f"  → {w['pubkey'][:8]}... [red]ошибка соединения[/red]")
        console.print("[green]Фандинг завершён[/green]")

    # ====================== BALANCES ======================
    def get_balances(self):
        console.print(Panel.fit("[bold cyan]Реальные балансы кошельков (SOL)[/bold cyan]", border_style="cyan"))
        for w in self.wallets:
            try:
                payload = {"jsonrpc": "2.0", "id": 1, "method": "getBalance", "params": [w["pubkey"]]}
                r = requests.post(RPC_URL, json=payload, timeout=10)
                sol = r.json()["result"]["value"] / 1_000_000_000
                console.print(f"  {w['pubkey'][:8]}... → [green]{sol:.4f} SOL[/green]")
            except:
                console.print(f"  {w['pubkey'][:8]}... → [red]ошибка RPC[/red]")

    # ====================== WITHDRAW ALL ======================
    def withdraw_all(self):
        if not self.main_kp:
            console.print("[red]Главный ключ не найден![/red]")
            return
        console.print("[bold]Выводим ВСЕ SOL со всех кошельков на главный...[/bold]")
        for w in self.wallets:
            try:
                payload = {"side": "transfer", "to": str(self.main_kp.pubkey()), "amount": "all", "dry_run": False}
                r = requests.post(f"{EXECUTOR_URL}/trade", json=payload, timeout=30)
                if r.status_code == 200:
                    console.print(f"  → {w['pubkey'][:8]}... [green]вывод OK[/green]")
                else:
                    console.print(f"  → {w['pubkey'][:8]}... [red]ошибка[/red]")
            except:
                console.print(f"  → {w['pubkey'][:8]}... [red]таймаут[/red]")
        console.print("[green]Withdraw All завершён[/green]")

    # ====================== WALLET WARMUP 2.0 ======================
    def wallet_warmup(self, cycles: int = 5, intensity: str = "normal"):
        """
        intensity: "light" / "normal" / "heavy"
        """
        if intensity == "light":
            max_amount = 0.003
            swap_chance = 0.3
        elif intensity == "heavy":
            max_amount = 0.012
            swap_chance = 0.65
        else:  # normal
            max_amount = 0.006
            swap_chance = 0.5

        console.print(Panel.fit(
            f"[bold]Запускаю продвинутый прогрев кошельков (Warmup 3.0)\n"
            f"Циклов: {cycles} | Интенсивность: {intensity}\n"
            f"Включает Jupiter свопы и создание ATA[/bold]",
            title="Wallet Warmup 3.0",
            border_style="blue"
        ))

        for cycle in range(1, cycles + 1):
            console.print(f"[cyan]Цикл прогрева {cycle}/{cycles}...[/cyan]")
            random.shuffle(self.wallets)

            for i in range(len(self.wallets) - 1):
                from_w = self.wallets[i]
                to_w = self.wallets[i + 1]

                # 1. Перевод SOL (всегда)
                amount = round(random.uniform(0.0005, max_amount), 6)
                try:
                    payload = {"side": "transfer", "to": to_w["pubkey"], "amount": amount, "dry_run": False}
                    r = requests.post(f"{EXECUTOR_URL}/trade", json=payload, timeout=25)
                    if r.status_code == 200:
                        console.print(f"  SOL transfer → {amount} SOL [green]OK[/green]")
                    else:
                        console.print(f"  SOL transfer → ошибка")
                except:
                    console.print(f"  SOL transfer → таймаут")

                time.sleep(random.uniform(1.8, 5.5))

                # 2. Jupiter swap (с вероятностью)
                if random.random() < swap_chance:
                    try:
                        # Покупаем небольшой объём популярного токена (USDC → BONK или POPCAT и т.д.)
                        payload = {
                            "side": "buy",
                            "mint": "9z4e8qZ2z1z2z3z4z5z6z7z8z9z0z1z2z3z4z5z6z7z8z9z0",  # пример популярного токена
                            "amount_in": round(random.uniform(0.001, 0.004), 6),
                            "dry_run": False
                        }
                        r = requests.post(f"{EXECUTOR_URL}/trade", json=payload, timeout=30)
                        if r.status_code == 200:
                            console.print(f"  Jupiter swap → [green]OK[/green]")
                        else:
                            console.print(f"  Jupiter swap → ошибка")
                    except:
                        console.print(f"  Jupiter swap → таймаут")

                    time.sleep(random.uniform(3.0, 8.0))

            # Большая пауза между циклами
            time.sleep(random.uniform(20, 45))

        console.print("[green]✅ Продвинутый Warmup 3.0 успешно завершён![/green]")
        console.print("[yellow]Кошельки теперь значительно живее для фильтров.[/yellow]")

    # ====================== LAUNCH WITH ANTI-DETECT ======================
    def launch(self, name: str, symbol: str, description: str, image_path: Path, 
               buy_sol_per_wallet: float = 0.03, anti_level: str = "medium"):
        
        if not self.wallets:
            self.generate_wallets(15)

        # === АНТИ-ДЕТЕКТ ===
        wallets_to_use = self.wallets.copy()
        buy_amounts = []
        jito_tips = []

        if anti_level == "low":
            random.shuffle(wallets_to_use)
            deviation = 0.07
            for _ in wallets_to_use:
                buy_amounts.append(round(buy_sol_per_wallet * random.uniform(1 - deviation, 1 + deviation), 5))
                jito_tips.append(round(random.uniform(0.0008, 0.0018), 6))

        elif anti_level == "medium":
            random.shuffle(wallets_to_use)
            deviation = 0.16
            for _ in wallets_to_use:
                buy_amounts.append(round(buy_sol_per_wallet * random.uniform(1 - deviation, 1 + deviation), 5))
                jito_tips.append(round(random.uniform(0.0005, 0.0025), 6))

        else:  # high
            random.shuffle(wallets_to_use)
            deviation = 0.24
            for _ in wallets_to_use:
                buy_amounts.append(round(buy_sol_per_wallet * random.uniform(1 - deviation, 1 + deviation), 5))
                jito_tips.append(round(random.uniform(0.0004, 0.0035), 6))

        avg_buy = sum(buy_amounts) / len(buy_amounts)
        real_spread = round(abs((avg_buy / buy_sol_per_wallet) - 1) * 100)

        console.print(Panel.fit(
            f"[bold]Анти-детект уровень:[/bold] [cyan]{anti_level.upper()}[/cyan]\n"
            f"Кошельки перемешаны: [green]Да[/green]\n"
            f"Разброс сумм покупки: [yellow]±{real_spread}%[/yellow]\n"
            f"Jito tips: разные (от {min(jito_tips):.5f} до {max(jito_tips):.5f} SOL)",
            title="🛡️ ANTI-DETECT",
            border_style="blue"
        ))

        total = 0.025 + sum(buy_amounts) + sum(jito_tips)
        console.print(Panel.fit(
            f"Создание токена:     0.025 SOL\n"
            f"Покупки (с jitter):  {sum(buy_amounts):.3f} SOL\n"
            f"Jito tips (разные):  {sum(jito_tips):.4f} SOL\n"
            f"[bold yellow]Итого ≈ {total:.3f} SOL[/bold yellow]",
            title="Расчёт расходов",
            border_style="yellow"
        ))

        if Prompt.ask("Запустить бандл?", choices=["y", "n"], default="y") == "n":
            return

        payload = {
            "name": name,
            "symbol": symbol,
            "description": description,
            "image_path": str(image_path.absolute()),
            "wallets": wallets_to_use,
            "buy_amounts": buy_amounts,        # новое поле — индивидуальные суммы
            "jito_tips": jito_tips,            # новое поле
            "dry_run": self.control.get("trading", {}).get("dry_run", True)
        }

        try:
            r = requests.post(f"{EXECUTOR_URL}/launch_bundle", json=payload, timeout=120)
            if r.status_code == 200:
                data = r.json()
                mint = data.get('mint')
                logger.success("✅ Bundle отправлен с анти-детектом!")
                logger.info(f"Mint: {mint}")
                logger.info(f"Bundle: {data.get('bundle_sig')}")

                self._save_last_mint(mint)

                history_entry = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "name": name,
                    "symbol": symbol,
                    "mint": mint,
                    "anti_level": anti_level,
                    "buy_per_wallet_base": buy_sol_per_wallet
                }
                self._save_launch_history(history_entry)

                # Автозапуск Volume Maker
                if Prompt.ask("\nЗапустить Volume Maker сразу с этим mint’ом? (y/n)", choices=["y", "n"], default="y") == "y":
                    minutes = int(Prompt.ask("Сколько минут volume?", default="30"))
                    trade_sol = float(Prompt.ask("Объём за трейд (SOL)", default="0.01"))
                    self.volume_running = True
                    def volume_wrapper():
                        try:
                            self.start_volume_maker(minutes, trade_sol, mint=mint)
                        finally:
                            self.volume_running = False
                    self.volume_thread = threading.Thread(target=volume_wrapper, daemon=True)
                    self.volume_thread.start()
                    console.print("[green]✅ Volume Maker запущен в фоне[/green]")
            else:
                logger.error(f"Ошибка: {r.text}")
        except Exception as e:
            logger.error(f"Не удалось подключиться к executor: {e}")

    # ====================== SELL ALL ======================
    def sell_all(self, mint: str):
        self.emergency_stop()
        console.print(f"[bold magenta]Продаём ВСЕ токены {mint}...[/bold magenta]")
        for w in self.wallets:
            try:
                payload = {"side": "sell", "mint": mint, "amount_in": "all", "dry_run": False}
                r = requests.post(f"{EXECUTOR_URL}/trade", json=payload, timeout=30)
                if r.status_code == 200:
                    console.print(f"  → {w['pubkey'][:8]}... [green]продано[/green]")
                else:
                    console.print(f"  → {w['pubkey'][:8]}... [red]ошибка[/red]")
            except:
                console.print(f"  → {w['pubkey'][:8]}... [red]ошибка[/red]")
        console.print("[green]Sell All завершён[/green]")

    # ====================== AUTO SELL WITH TRAILING ======================
    def auto_sell_tp(self, mint: str, tp_percent: float = 100.0, trailing_percent: float = 30.0):
        if self.auto_sell_running:
            console.print("[red]⚠️ Auto Sell уже запущен! Сначала останови предыдущий.[/red]")
            return

        self.auto_sell_running = True
        self.tp_percent = tp_percent
        self.trailing_percent = trailing_percent

        console.print(Panel.fit(
            f"[bold green]Auto Sell + Trailing запущен[/bold green]\n"
            f"Токен: [cyan]{mint[:8]}...[/cyan]\n"
            f"TP: +{tp_percent:.1f}% от базовой цены\n"
            f"Trailing Stop: -{trailing_percent:.1f}% от максимума",
            title="🚀 AUTO SELL TP + TRAILING",
            border_style="green"
        ))

        def monitor_price():
            base_price = None
            max_price = 0.0
            last_log_time = 0

            while self.auto_sell_running:
                try:
                    r = requests.get(f"{EXECUTOR_URL}/state?mint={mint}", timeout=8)
                    if r.status_code == 200:
                        data = r.json().get("state", {})
                        sol_res = float(data.get("virtualSolReservesSol", 0))
                        token_res = float(data.get("virtualTokenReserves", 1))
                        current_price = sol_res / token_res if token_res > 0 else 0

                        if current_price <= 0:
                            time.sleep(3)
                            continue

                        if base_price is None:
                            base_price = current_price
                            max_price = current_price
                            logger.success(f"✅ Базовая цена зафиксирована: {base_price:.8f} SOL")

                        if current_price > max_price:
                            max_price = current_price

                        # ==================== TAKE PROFIT ====================
                        if current_price >= base_price * (1 + self.tp_percent / 100):
                            logger.success(f"🎯 TAKE PROFIT +{self.tp_percent}% ДОСТИГНУТ! Продаём ВСЁ...")
                            self.sell_all(mint)
                            self.auto_sell_running = False
                            break

                        # ==================== TRAILING STOP ====================
                        trailing_trigger = max_price * (1 - self.trailing_percent / 100)
                        if current_price <= trailing_trigger and max_price >= base_price * 1.08:  # защита от шума
                            logger.warning(f"⛔ TRAILING STOP (-{self.trailing_percent}%) СРАБОТАЛ! Продаём ВСЁ...")
                            self.sell_all(mint)
                            self.auto_sell_running = False
                            break

                        # Лог цены раз в 8 секунд (не спамит)
                        if time.time() - last_log_time > 8:
                            logger.info(f"💰 Цена: {current_price:.8f} SOL | Max: {max_price:.8f} | TP: +{self.tp_percent}%")
                            last_log_time = time.time()

                except Exception as e:
                    logger.error(f"Ошибка мониторинга цены: {e}")

                time.sleep(2)  # проверка каждые 3 секунды — почти без задержки

            logger.info("🛑 Мониторинг Auto Sell остановлен")

        # Запуск в фоне
        self.auto_sell_thread = threading.Thread(target=monitor_price, daemon=True)
        self.auto_sell_thread.start()

        console.print("[dim]✅ Мониторинг запущен в фоне. Закрой окно — авто-селл остановится.[/dim]")
    def stop_auto_sell(self):
        if self.auto_sell_running:
            self.auto_sell_running = False
            console.print("[yellow]🛑 Auto Sell остановлен пользователем[/yellow]")
            
            if self.auto_sell_thread and self.auto_sell_thread.is_alive():
                self.auto_sell_thread.join(timeout=2.0)   # мягко ждём завершения
            
            logger.success("✅ Auto Sell полностью остановлен")
        else:
            console.print("[dim]Auto Sell не был запущен[/dim]")
    # ====================== VOLUME MAKER ======================
    def start_volume_maker(self, minutes: int = 30, trade_sol: float = 0.01, mint: str = None):
        if not self.wallets:
            logger.error("Нет кошельков")
            return

        # Определяем mint
        if mint is None:
            last_mint = self._load_last_mint()
            if last_mint:
                use_last = Prompt.ask(f"Использовать последний mint {last_mint[:8]}...? (y/n)", choices=["y","n"], default="y")
                mint = last_mint if use_last == "y" else Prompt.ask("Введи mint токена")
            else:
                mint = Prompt.ask("Введи mint токена")

        # Инициализируем позиции (если первый запуск)
        if not self.wallet_positions:
            for w in self.wallets:
                self.wallet_positions[w["pubkey"]] = 0.0

        self.volume_running = True
        self.volume_start_time = time.time()
        self.volume_minutes = minutes
        self.volume_logs.clear()

        logger.info(f"🚀 Умный Volume Maker запущен на {minutes} минут по токену {mint[:8]}...")
        end_time = time.time() + minutes * 60
        cycle = 0

        while time.time() < end_time:
            if not self.volume_running:
                self.volume_logs.append(f"[{time.strftime('%H:%M:%S')}] 🛑 Volume Maker остановлен по запросу")
                break

            cycle += 1
            self.volume_logs.append(f"[{time.strftime('%H:%M:%S')}] Цикл {cycle}")
            if len(self.volume_logs) > 15:
                self.volume_logs.pop(0)

            # Умная логика для каждого кошелька
            for w in self.wallets:
                pubkey = w["pubkey"]
                current_tokens = self.wallet_positions.get(pubkey, 0.0)

                # Определяем, что делать
                if current_tokens < 0.3:           # мало токенов → покупаем
                    side = "buy"
                    amount = trade_sol
                elif current_tokens > 0.7:         # много токенов → продаём часть
                    side = "sell"
                    amount = "all"                 # продаём всё, но потом сразу докупим в следующем цикле
                else:
                    side = random.choice(["buy", "sell"])
                    amount = trade_sol if side == "buy" else "all"

                try:
                    payload = {
                        "side": side,
                        "mint": mint,
                        "amount_in": amount,
                        "dry_run": False
                    }
                    r = requests.post(f"{EXECUTOR_URL}/trade", json=payload, timeout=20)
                    status = "OK" if r.status_code == 200 else "ошибка"

                    # Обновляем позицию (примерно)
                    if side == "buy":
                        self.wallet_positions[pubkey] += 0.6   # условно +60% позиции
                    else:
                        self.wallet_positions[pubkey] = max(0.0, self.wallet_positions[pubkey] * 0.4)  # оставляем ~40%

                    log = f"  {pubkey[:6]}... {side.upper()} {status}"
                    self.volume_logs.append(log)
                    if len(self.volume_logs) > 15:
                        self.volume_logs.pop(0)

                except:
                    self.volume_logs.append(f"  {pubkey[:6]}... таймаут")

                time.sleep(random.uniform(1.5, 4.0))

            time.sleep(random.uniform(8, 18))

        self.volume_running = False
        self.volume_start_time = None
        logger.success("✅ Умный Volume Maker завершён")

    def status(self):
        console.print(f"[bold]Кошельков:[/bold] {len(self.wallets)}")
        for w in self.wallets[:10]:
            console.print(f"  {w['pubkey'][:8]}...{w['pubkey'][-6:]}")

    def show_launch_history(self):
        if not HISTORY_PATH.exists():
            console.print("[yellow]История запусков пуста[/yellow]")
            return
        history = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        console.print(Panel.fit("[bold]Последние запуски[/bold]", border_style="blue"))
        for entry in reversed(history[-10:]):
            console.print(f"[{entry['timestamp']}] {entry['name']} ({entry['symbol']}) → {entry.get('mint', 'N/A')[:8]}...")
    # ====================== EMERGENCY STOP (Dump All) ======================
    def emergency_stop(self):
        """Останавливает ВСЁ при нажатии Dump All"""
        stopped = False

        if self.auto_sell_running:
            self.stop_auto_sell()
            stopped = True

        if self.volume_running:
            self.stop_volume_maker()
            stopped = True

        if self.launch_running:
            self.stop_launch()
            stopped = True

        if stopped:
            console.print("[red bold]🚨 EMERGENCY STOP: все процессы остановлены[/red bold]")
        else:
            console.print("[dim]Ничего не было запущено[/dim]")

    # ====================== STOP LAUNCH ======================
    def stop_launch(self):
        if self.launch_running:
            self.launch_running = False
            console.print("[yellow]🛑 Launch остановлен[/yellow]")
            if self.launch_thread and self.launch_thread.is_alive():
                self.launch_thread.join(timeout=3.0)
            logger.success("✅ Launch полностью остановлен")
        else:
            console.print("[dim]Launch не запущен[/dim]")

    # ====================== STOP VOLUME ======================
    def stop_volume_maker(self):
        if self.volume_running:
            self.volume_running = False
            console.print("[yellow]🛑 Volume Maker остановлен пользователем[/yellow]")
            
            if self.volume_thread and self.volume_thread.is_alive():
                self.volume_thread.join(timeout=3.0)
            
            # Полная очистка логов
            self.volume_logs.clear()
            self.volume_start_time = None
            self.volume_minutes = 0
            
            logger.success("✅ Volume Maker полностью остановлен")
        else:
            console.print("[dim]Volume Maker не запущен[/dim]")
    # ====================== VOLUME STATUS DASHBOARD ======================
    def show_volume_status(self):
        if not self.volume_running or self.volume_start_time is None:
            console.print("[yellow]Volume Maker сейчас не запущен[/yellow]")
            return

        elapsed = int(time.time() - self.volume_start_time)
        remaining = max(0, self.volume_minutes * 60 - elapsed)
        min_left = remaining // 60
        sec_left = remaining % 60

        console.print(Panel.fit(
            f"[bold cyan]Volume Maker работает[/bold cyan]\n"
            f"Прошло: [white]{elapsed//60}м {elapsed%60}с[/white]\n"
            f"Осталось: [green]{min_left}м {sec_left}с[/green]\n"
            f"Циклов: {len([l for l in self.volume_logs if 'Цикл' in l])}",
            title="📊 VOLUME MAKER STATUS",
            border_style="cyan"
        ))

        console.print("[bold]Последние действия:[/bold]")
        for log in self.volume_logs[-12:]:
            console.print(log)

        console.print("\n[red bold]6. 🛑 Stop Volume Maker[/red bold]")
        console.print("[dim]Нажми 6 или back для возврата[/dim]")  
    # ====================== MAIN WALLET STATUS ======================
    def get_main_wallet_status(self):
        if not self.main_kp:
            console.print("[red]❌ Главный ключ не найден![/red]")
            return

        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBalance",
                "params": [str(self.main_kp.pubkey())]
            }
            r = requests.post(RPC_URL, json=payload, timeout=10)
            sol = r.json()["result"]["value"] / 1_000_000_000

            console.print(Panel.fit(
                f"[bold cyan]Главный кошелёк (Main Wallet)[/bold cyan]\n"
                f"Адрес: [white]{self.main_kp.pubkey()}[/white]\n"
                f"Баланс: [bold green]{sol:.6f} SOL[/bold green]",
                title="🔑 Main Wallet Status",
                border_style="cyan"
            ))
        except Exception as e:
            console.print(f"[red]Ошибка получения баланса: {e}[/red]")       

def main():
    pass