"""Executa os harnesses Node (DOM e fetch simulados, sem rede) do módulo inscricoes."""

import os
import shutil
import subprocess
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _rodar(nome):
    return subprocess.run(["node", os.path.join(RAIZ, "tests", "js", nome)],
                          capture_output=True, text=True, timeout=60, encoding="utf-8")


@unittest.skipUnless(shutil.which("node"), "node não instalado")
class InscricoesJsTest(unittest.TestCase):
    def test_sessao_siaa_cartao_inscricoes(self):
        r = _rodar("siaa_sessao_inscricoes_harness.js")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_conexao_siaa_cartao_inscricoes(self):
        r = _rodar("conexao_siaa_inscricoes_harness.js")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
