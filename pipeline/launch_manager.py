import json
import time
import random
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
EXECUTOR_URL = "http://127.0.0.1:8790"

class LaunchManager:
    def __init__(self):
        self.control = load_control()
        self.wallets: List[Dict] = self._load_wallets()
        self.main_kp = self._load_main_keypair()

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
                payload = {
                    "side": "transfer",
                    "to": w["pubkey"],
                    "amount": sol_amount,
                    "dry_run": False
                }
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
        console.print(Panel.fit("[bold cyan]Балансы кошельков[/bold cyan]", border_style="cyan"))
        for w in self.wallets:
            console.print(f"  {w['pubkey'][:8]}... → [dim]0.0000 SOL (реальный чек скоро)[/dim]")

    # ====================== WITHDRAW ======================
    def withdraw_all(self):
        console.print("[bold]Выводим всё на главный кошелёк...[/bold]")
        console.print("[yellow]Withdraw All будет добавлен в следующем обновлении[/yellow]")

    # ====================== LAUNCH ======================
    def launch(self, name: str, symbol: str, description: str, image_path: Path, buy_sol_per_wallet: float = 0.03):
        if not self.wallets:
            self.generate_wallets(15)

        total = 0.025 + (buy_sol_per_wallet * len(self.wallets)) + 0.003
        console.print(Panel.fit(
            f"Создание токена:     0.025 SOL\n"
            f"Покупки:             {buy_sol_per_wallet*len(self.wallets):.3f} SOL\n"
            f"Jito tip:            0.003 SOL\n"
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
            "buy_sol_per_wallet": buy_sol_per_wallet,
            "wallets": self.wallets,
            "dry_run": self.control.get("trading", {}).get("dry_run", True)
        }

        try:
            r = requests.post(f"{EXECUTOR_URL}/launch_bundle", json=payload, timeout=120)
            if r.status_code == 200:
                data = r.json()
                logger.success("✅ Bundle отправлен!")
                logger.info(f"Mint: {data.get('mint')}")
                logger.info(f"Bundle: {data.get('bundle_sig')}")
            else:
                logger.error(f"Ошибка: {r.text}")
        except Exception as e:
            logger.error(f"Не удалось подключиться к executor: {e}")

    # ====================== SELL ALL ======================
    def sell_all(self, mint: str):
        console.print(f"[bold magenta]Продаём ВСЕ токены {mint} со всех кошельков...[/bold magenta]")
        for w in self.wallets:
            try:
                payload = {
                    "side": "sell",
                    "mint": mint,
                    "amount_in": "all",
                    "dry_run": False
                }
                r = requests.post(f"{EXECUTOR_URL}/trade", json=payload, timeout=30)
                if r.status_code == 200:
                    console.print(f"  → {w['pubkey'][:8]}... [green]продано[/green]")
                else:
                    console.print(f"  → {w['pubkey'][:8]}... [red]ошибка[/red]")
            except:
                console.print(f"  → {w['pubkey'][:8]}... [red]ошибка[/red]")
        console.print("[green]Sell All завершён[/green]")

    # ====================== VOLUME MAKER ======================
    def start_volume_maker(self, minutes: int = 30, trade_sol: float = 0.01):
        if not self.wallets:
            logger.error("Нет кошельков")
            return

        logger.info(f"🚀 Volume Maker запущен на {minutes} минут ({trade_sol} SOL за трейд)")
        end_time = time.time() + minutes * 60
        cycle = 0

        while time.time() < end_time:
            cycle += 1
            logger.info(f"Цикл {cycle}...")

            for w in self.wallets:
                side = random.choice(["buy", "sell"])
                try:
                    payload = {
                        "side": side,
                        "mint": "SIMULATED_VOLUME_MINT",
                        "amount_in": trade_sol if side == "buy" else "all",
                        "dry_run": False
                    }
                    r = requests.post(f"{EXECUTOR_URL}/trade", json=payload, timeout=20)
                    status = "[green]OK[/green]" if r.status_code == 200 else "[red]ошибка[/red]"
                    console.print(f"  {w['pubkey'][:6]}... {side.upper()} {status}")
                except:
                    console.print(f"  {w['pubkey'][:6]}... [red]таймаут[/red]")

                time.sleep(random.uniform(1.2, 3.8))

            time.sleep(random.uniform(10, 20))

        logger.success(f"Volume Maker завершён после {minutes} минут")

    def status(self):
        console.print(f"[bold]Кошельков:[/bold] {len(self.wallets)}")
        for w in self.wallets[:10]:
            console.print(f"  {w['pubkey'][:8]}...{w['pubkey'][-6:]}")

def main():
    pass