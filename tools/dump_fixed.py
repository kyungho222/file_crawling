import importlib
import sys
import os

# Ensure workspace root is on path
sys.path.insert(0, os.getcwd())

from backend.edu_url_patch import _fix_cancel_block

with open(os.path.join("edu", "url_edu.py"), "r", encoding="utf-8") as f:
    src = f.read()

fixed = _fix_cancel_block(src)
with open(os.path.join("tools", "fixed_source_snippet.txt"), "w", encoding="utf-8") as out:
    out.write(fixed)

print("wrote tools/fixed_source_snippet.txt")

