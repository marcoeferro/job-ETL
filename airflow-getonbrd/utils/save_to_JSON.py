import json
from pathlib import Path

def save_to_JSON(data, path):
    try:
        base_path = Path(path).parent.parent 
        path = base_path / path
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Error saving JSON to {path}: {e}")

    print(f"Saved page to {path}")

