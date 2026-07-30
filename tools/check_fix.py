import textwrap, sys, io, os

import importlib, sys, os

# reuse the implementation from backend.edu_url_patch to ensure parity
sys.path.insert(0, os.getcwd())
from backend.edu_url_patch import _fix_cancel_block


def main():
    path = os.path.join("edu", "url_edu.py")
    with open(path, "r", encoding="utf-8") as f:
        src = f.read()

    fixed = _fix_cancel_block(src)
    try:
        compile(textwrap.dedent(fixed), "<string>", "exec")
        print("COMPILED_OK")
    except IndentationError as e:
        print("IndentationError:", e)
        lines = fixed.splitlines()
        lineno = e.lineno
        start = max(0, lineno - 6)
        end = min(len(lines), lineno + 5)
        for i in range(start, end):
            prefix = "->" if i + 1 == lineno else "  "
            print(f"{prefix} {i+1}: {lines[i]!r}")
        return 1

if __name__ == "__main__":
    sys.exit(main())

