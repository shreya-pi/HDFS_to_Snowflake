import os

EXCLUDE_DIRS = {"myenv", "__pycache__", ".git", "converted_procedures", "deployed_procedures", "extracted_procedures", "processed_procedures"}

with open("structure.txt", "w") as f:
    for root, dirs, files in os.walk("."):
        # Filter out excluded directories
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

        level = root.count(os.sep)
        indent = "    " * level
        f.write(f"{indent}{os.path.basename(root)}/\n")
        for file in files:
            f.write(f"{indent}    {file}\n")