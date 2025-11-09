from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


def update_run_tests() -> None:
    # Run pytest to get failing tests
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "narwhals/tests",
            "-p",
            "pytest_constructor_override",
            "--use-external-constructor",
            "--tb",
            "no",
            "-v",
            "--constructors",
            "daft",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    # Extract failed test names using regex
    failed_tests = re.findall(
        r"(?:FAILED|ERROR) narwhals/tests/.*\.py::(\w+)\[?", result.stdout
    )

    # Sort and format the test names
    formatted_tests = " or \\\n".join(sorted(failed_tests))

    # Read the current run_tests.sh
    run_tests_path = Path("run_tests.sh")
    content = run_tests_path.read_text(encoding="utf-8")

    # Replace the TESTS_THAT_NEED_FIX content
    new_content = re.sub(
        r'TESTS_THAT_NEED_FIX=" \\(.*?)"',
        f'TESTS_THAT_NEED_FIX=" \\\n{formatted_tests} \\\n"',
        content,
        flags=re.DOTALL,
    )

    # Write back to run_tests.sh
    run_tests_path.write_text(new_content, encoding="utf-8")

    print("Updated run_tests.sh with new failing tests")


if __name__ == "__main__":
    update_run_tests()
