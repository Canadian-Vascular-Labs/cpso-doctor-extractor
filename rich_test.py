import time

from rich.progress import Progress


with Progress(transient=False) as progress:

    task1 = progress.add_task("[red]Downloading...", total=10)
    task2 = progress.add_task("[green]Processing...", total=10)
    task3 = progress.add_task("[cyan]Cooking...", total=10)

    while not progress.finished:
        progress.update(task1, advance=0.5)
        progress.update(task2, advance=0.3)
        progress.update(task3, advance=0.9)
        time.sleep(0.02)
