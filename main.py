import argparse
import threading

from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich import print as rprint

from pipeline.launch_manager import LaunchManager
from utils.logger import setup_logger

console = Console()
logger = setup_logger("INFO")

def clear():
    console.clear()

def main_menu(manager: LaunchManager):
    while True:
        clear()
        console.print(Panel.fit(
            "[bold cyan]SOLANA PUMP.FUN BUNDLE BOT[/bold cyan]\n",
            title="Main Menu",
            border_style="green"
        ))

        rprint("\n[bold]Options:[/bold]")
        rprint("1. [blue]Generate Wallets[/blue]")
        rprint("2. [green]Wallet Management[/green]")
        rprint("3. [yellow]Launch Manager[/yellow]")
        rprint("4. [magenta]Sell Menu[/magenta]")
        if manager.volume_running:
            rprint("5. [cyan]📊 Volume Status (running)[/cyan]")
        else:
            rprint("5. [cyan]Start Volume Maker[/cyan]")
        rprint("6. [white]History of Launches[/white]")
        rprint("7. [white]Status & Info[/white]")
        rprint("8. [red]Exit[/red]")

        try:
            choice = Prompt.ask("Choose an option", choices=["1","2","3","4","5","6","7","8"])
        except ValueError:
            console.print("[red]Please select one of the available options[/red]")
            continue

        if choice == "1":
            num = int(Prompt.ask("How many wallets?", default="15"))
            force = Prompt.ask("Force regenerate? (y/n)", choices=["y","n"]) == "y"
            manager.generate_wallets(num, force=force)
            Prompt.ask("\nPress Enter to continue...")

        elif choice == "2":
            wallet_menu(manager)
        elif choice == "3":
            launch_menu(manager)
        elif choice == "4":
            sell_menu(manager)
        elif choice == "5":
            if manager.volume_running:
                manager.show_volume_status()
                
                # Правильная обработка выбора
                action = Prompt.ask(
                    "Выберите действие", 
                    choices=["6", "back"], 
                    default="back",
                    show_choices=False
                )
                if action == "6":
                    manager.stop_volume_maker()
                Prompt.ask("\nPress Enter to continue...")
            else:
                minutes = int(Prompt.ask("Сколько минут volume?", default="30"))
                trade_sol = float(Prompt.ask("Объём за трейд (SOL)", default="0.01"))

                last_mint = manager._load_last_mint()
                use_last = Prompt.ask(f"Использовать последний mint {last_mint[:10]}...? (y/n)", default="y").lower() == "y"
                
                if not use_last:
                    mint_str = Prompt.ask("Введите mint address")
                    manager.mint = mint_str
                else:
                    manager.mint = last_mint

                # Валидация mint
                if not manager._is_valid_mint(manager.mint):
                    console.print("[red]Invalid mint! Пожалуйста, введите реальный mint.[/red]")
                    Prompt.ask("\nPress Enter to continue...")
                    continue  # Не запускаем

                manager._save_last_mint(manager.mint)

                # Теперь запускаем thread
                manager.volume_running = True
                manager.volume_thread = threading.Thread(target=manager.start_volume_maker, args=(minutes, trade_sol, manager.mint), daemon=True)
                manager.volume_thread.start()

                console.print(f"[green]✅ Volume Maker запущен на {minutes} минут по токену {manager.mint[:10]}...[/green]")
                Prompt.ask("\nPress Enter to continue...")
        elif choice == "6":
            manager.show_launch_history()
            Prompt.ask("\nPress Enter to continue...")    
        elif choice == "7":
            manager.status()
            Prompt.ask("Press Enter...")
        elif choice == "8":
            console.print("[red]Выход...[/red]")
            break

# ====================== WALLET MANAGEMENT ======================
def wallet_menu(manager: LaunchManager):
    while True:
        clear()
        console.print(Panel.fit("[bold green]Wallet Management[/bold green]", border_style="green"))

        rprint("1. Fund Wallets")
        rprint("2. [cyan]Main Wallet Status[/cyan]")
        rprint("3. Generated Wallets Balance")
        rprint("4. Transfer Tokens")
        rprint("5. Refund SOL")
        rprint("6. Wallet Warmup")
        rprint("7. Wallet Cleanup")
        rprint("8. Back to Main Menu")

        choice = Prompt.ask("Choose", choices=["1","2","3","4","5","6","7","8"])

        if choice == "1":
            amount = float(Prompt.ask("SOL per wallet", default="0.5"))
            manager.fund_all(amount)
        elif choice == "2":                                   
            manager.get_main_wallet_status()    
        elif choice == "3":
            manager.get_balances()
        elif choice == "5":
            manager.withdraw_all()
        elif choice == "6":
            cycles = int(Prompt.ask("Сколько циклов прогрева?", default="4"))
            amount = float(Prompt.ask("Макс. сумма за трансфер (SOL)", default="0.008"))
            manager.wallet_warmup(cycles, amount)    
        elif choice == "7":
            if Prompt.ask("Удалить все кошельки? (y/n)", choices=["y","n"]) == "y":
                Path("data/wallets.json").unlink(missing_ok=True)
                manager.wallets.clear()
                console.print("[green]Кошельки очищены![/green]")
        elif choice == "8":
            break

        Prompt.ask("\nPress Enter to continue...")

# ====================== LAUNCH MANAGER ======================
def launch_menu(manager: LaunchManager):
    while True:
        clear()
        console.print(Panel.fit("[bold yellow]Launch Manager[/bold yellow]", border_style="yellow"))

        rprint("1. Launch Pump (Bundler)")
        rprint("2. Back to Main Menu")

        choice = Prompt.ask("Choose", choices=["1","2"])

        if choice == "1":
            name = Prompt.ask("Token name", default="Dean W")
            symbol = Prompt.ask("Symbol", default="DW")
            desc = Prompt.ask("Description", default="Family Business")
            image = Prompt.ask("Image path", default=r"D:\Aladdin\memes\dean-winchester.jpeg")
            buy = float(Prompt.ask("Buy per wallet (SOL)", default="0.03"))

            anti_level = Prompt.ask(
                "Anti-detect level",
                choices=["low", "medium", "high"],
                default="medium"
            )

            manager.launch(name, symbol, desc, Path(image), buy, anti_level=anti_level)

        elif choice == "2":
            break

# ====================== SELL MENU ======================
def sell_menu(manager: LaunchManager):
    while True:
        clear()
        console.print(Panel.fit("[bold magenta]Sell Menu[/bold magenta]", border_style="magenta"))

        rprint("1. Dump All")
        rprint("2. Dump %")
        rprint("3. Single Wallet Sell")
        rprint("4. Auto Sell by TP/SL + Trailing")

        if manager.auto_sell_running:
            rprint("5. [red bold]🛑 Stop Auto Sell[/red bold]")
            rprint("6. Back to Main Menu")
            choices = ["1", "2", "3", "4", "5", "6"]
        else:
            rprint("5. Back to Main Menu")
            choices = ["1", "2", "3", "4", "5"]

        choice = Prompt.ask("Choose", choices=choices)

        if choice == "1":
            mint = Prompt.ask("Mint address")
            manager.sell_all(mint)

        elif choice in ["2", "3"]:
            console.print("[yellow]Эта функция скоро будет добавлена[/yellow]")

        elif choice == "4":
            mint = Prompt.ask("Mint токена")
            tp = float(Prompt.ask("TP % (50, 100, 200...)", default="100"))
            trailing = float(Prompt.ask("Trailing Stop %", default="30"))
            manager.auto_sell_tp(mint, tp, trailing if trailing else 0)

        elif choice == "5" and manager.auto_sell_running:
            manager.stop_auto_sell()

        elif (choice == "5" and not manager.auto_sell_running) or choice == "6":
            break

        Prompt.ask("\nPress Enter to continue...")

def main():
    manager = LaunchManager()
    main_menu(manager)

if __name__ == "__main__":
    main()