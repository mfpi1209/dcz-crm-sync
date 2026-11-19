"""Executa o harness Node (DOM e fetch simulados, sem rede) da renovação automática."""

import os
import shutil
import subprocess
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@unittest.skipUnless(shutil.which("node"), "node não instalado")
class RenovacaoJsTest(unittest.TestCase):
    def test_harness(self):
        r = subprocess.run(["node", os.path.join(RAIZ, "tests", "js", "conexao_siaa_renovacao_harness.js")],
                           capture_output=True, text=True, timeout=60, encoding="utf-8")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_harness_seletiva(self):
        r = subprocess.run(["node", os.path.join(RAIZ, "tests", "js", "conexao_siaa_renovacao_seletiva_harness.js")],
                           capture_output=True, text=True, timeout=60, encoding="utf-8")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
