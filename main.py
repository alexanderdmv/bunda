import argparse
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
            "[bold cyan]SOLANA PUMP.FUN BUNDLE BOT[/bold cyan]\n"
            "[white]by Grok + alexanderdmv[/white]",
            title="Main Menu",
            border_style="green"
        ))

        rprint("\n[bold]Options:[/bold]")
        rprint("1. [blue]Generate Wallets[/blue]")
        rprint("2. [green]Wallet Management[/green]")
        rprint("3. [yellow]Launch Manager[/yellow]")
        rprint("4. [magenta]Sell Menu[/magenta]")
        rprint("5. [cyan]Volume Maker[/cyan] (скоро)")
        rprint("6. [white]Status & Info[/white]")
        rprint("7. [red]Exit[/red]")

        choice = Prompt.ask("Choose an option", choices=["1","2","3","4","5","6","7"])

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
            minutes = int(Prompt.ask("Сколько минут volume?", default="30"))
            trade_sol = float(Prompt.ask("Объём за трейд (SOL)", default="0.01"))
            manager.start_volume_maker(minutes, trade_sol)
            Prompt.ask("\nPress Enter to continue...")
        elif choice == "6":
            manager.status()
            Prompt.ask("Press Enter...")
        elif choice == "7":
            console.print("[red]Выход...[/red]")
            break

# ====================== WALLET MANAGEMENT ======================
def wallet_menu(manager: LaunchManager):
    while True:
        clear()
        console.print(Panel.fit("[bold green]Wallet Management[/bold green]", border_style="green"))

        rprint("1. Fund Wallets (f)")
        rprint("2. Balance (b)")
        rprint("3. Transfer Tokens (t)")
        rprint("4. Refund SOL (r)")
        rprint("5. Wallet Cleanup (k)")
        rprint("6. Back to Main Menu (e)")

        choice = Prompt.ask("Choose", choices=["1","2","3","4","5","6"])

        if choice == "1":
            amount = float(Prompt.ask("SOL per wallet", default="0.5"))
            manager.fund_all(amount)
        elif choice == "2":
            manager.get_balances()
        elif choice == "4":
            manager.withdraw_all()
        elif choice == "5":
            if Prompt.ask("Удалить все кошельки? (y/n)", choices=["y","n"]) == "y":
                Path("data/wallets.json").unlink(missing_ok=True)
                manager.wallets.clear()
                console.print("[green]Кошельки очищены![/green]")
        elif choice == "6":
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
            image = Prompt.ask("Image path", default=r"C:\Users\vrald\OneDrive\Desktop\dean-winchester.jpeg")
            buy = float(Prompt.ask("Buy per wallet (SOL)", default="0.03"))

            manager.launch(name, symbol, desc, Path(image), buy)
            Prompt.ask("\nPress Enter...")

        elif choice == "2":
            break

# ====================== SELL MENU ======================
def sell_menu(manager: LaunchManager):
    while True:
        clear()
        console.print(Panel.fit("[bold magenta]Sell Menu[/bold magenta]", border_style="magenta"))

        rprint("1. Dump All")
        rprint("2. Dump %")
        rprint("3. Delay Sell All")
        rprint("4. Single Wallet Sell")
        rprint("5. Auto Sell")
        rprint("6. Back to Main Menu")

        choice = Prompt.ask("Choose", choices=["1","2","3","4","5","6"])

        if choice == "1":
            mint = Prompt.ask("Mint address")
            manager.sell_all(mint)
        elif choice == "6":
            break
        else:
            console.print("[yellow]Эта функция скоро будет добавлена[/yellow]")

        Prompt.ask("\nPress Enter...")

def main():
    manager = LaunchManager()
    main_menu(manager)

if __name__ == "__main__":
    main()