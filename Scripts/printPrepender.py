import sys
import os


class PrintPrepender:
    stdout = sys.stdout

    def __init__(self, text_to_prepend):
        self.text_to_prepend = text_to_prepend
        self.buffer = [self.text_to_prepend]

    def write(self, text):
        lines = text.splitlines(keepends=True)
        for line in lines:
            self.buffer.append(line)
            self.flush()
            if line.endswith(os.linesep):
                self.buffer.append(self.text_to_prepend)

    def flush(self, *args):
        self.stdout.write("".join(self.buffer))
        self.stdout.flush()
        self.buffer.clear()
