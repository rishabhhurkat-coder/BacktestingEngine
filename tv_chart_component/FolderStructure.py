import os
from pathlib import Path
from collections import Counter
import subprocess


# --------------------------------------------------
# DETAILED MODE (Full tree using Windows tree)
# --------------------------------------------------
def show_detailed_tree(folder_path):
    try:
        subprocess.run(
            ["cmd", "/c", "tree", str(folder_path), "/f"],
            check=True
        )
    except subprocess.CalledProcessError:
        print("❌ Failed to execute tree command")


# --------------------------------------------------
# SUMMARISED MODE (Aggregated counts)
# --------------------------------------------------
def show_summarised_tree(folder_path, prefix=""):

    try:
        items = sorted(os.listdir(folder_path))
    except PermissionError:
        print(prefix + "🚫 Permission Denied")
        return

    files = []
    folders = []

    for item in items:
        full_path = os.path.join(folder_path, item)
        if os.path.isdir(full_path):
            folders.append(item)
        else:
            files.append(item)

    # ---- Folders ----
    for index, folder in enumerate(folders):
        connector = "└── " if index == len(folders) - 1 and not files else "├── "
        print(prefix + connector + "📁 " + folder)
        show_summarised_tree(
            os.path.join(folder_path, folder),
            prefix + ("    " if connector == "└── " else "│   ")
        )

    # ---- File Aggregation ----
    if files:
        ext_counter = Counter()

        for f in files:
            ext = Path(f).suffix.lower()
            ext_counter[ext] += 1

        for ext, count in ext_counter.items():

            if ext == ".csv":
                label = f"{count} CSV files"
            elif ext in (".xlsx", ".xls"):
                label = f"{count} Excel files"
            elif ext == ".py":
                label = f"{count} Python files"
            else:
                label = f"{count} {ext or 'no-extension'} files"

            print(prefix + "└── 📄 " + label)


# --------------------------------------------------
# MAIN CONTROL
# --------------------------------------------------
def show_project_structure():

    current_folder = Path(__file__).resolve().parent

    print("\nSelect View Mode:")
    print("1 → Detailed (All files)")
    print("2 → Summarised (Counts only)")

    choice = input("Choice: ").strip()

    print("\n📂 Project Structure\n│")

    if choice == "1":
        show_detailed_tree(current_folder)
    elif choice == "2":
        show_summarised_tree(current_folder)
    else:
        print("❌ Invalid selection")


# --------------------------------------------------
# RUN
# --------------------------------------------------
show_project_structure()
