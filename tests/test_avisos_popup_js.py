"""Executa o harness Node do popup de avisos (polling, não repetição, recuperado, manuais)."""

import os
import shutil
import subprocess
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@unittest.skipUnless(shutil.which("node"), "node não instalado")
class AvisosPopupJsTest(unittest.TestCase):
    def test_harness(self):
        r = subprocess.run(["node", os.path.join(RAIZ, "tests", "js", "avisos_popup_harness.js")],
                           capture_output=True, text=True, timeout=60, encoding="utf-8")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
