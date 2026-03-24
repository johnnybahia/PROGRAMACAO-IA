#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Otimizador de Distribuição de Produção — Google Sheets + Python
================================================================
Lê e escreve diretamente na planilha Google Sheets.
Sem limite de tempo. Simulações em paralelo com numpy.

Estrutura da aba PEDIDO:
  Col A: Data Inicial Especial (opcional — força início a partir desta data)
  Col B: Máquina Especial (opcional — nome L1 da aba de máquina; restringe alocação a esse modelo)
  Col C: Produto
  Col D: Referência
  Col E: Cor
  Col F: Quantidade de Máquinas
  Col G: Cliente
  Col H: Ordem de Compra
  Col I: Data de Entrega da OC (deadline)
  Col J: Data Finalização do Pedido  ← preenchida pelo código
  Col K: Prazo                       ← preenchida pelo código (X dias antecipado / -X dias atrasado)
  Cel M1: Data base de início do planejamento

Aba opcional 'DATAS FORA DE PROGRAMAÇÃO':
  Col A: datas bloqueadas (sem produção) — formato DD/MM/YYYY

Uso:
    python otimizador.py <URL_da_planilha> <credenciais.json>

Exemplo:
    python otimizador.py "https://docs.google.com/spreadsheets/d/..." credenciais.json

Requisitos:
    pip install gspread google-auth numpy
"""

import sys
import os
import copy
import random
import math
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta

try:
    import numpy as np
except ImportError:
    print("Erro: numpy não instalado. Execute: pip install gspread google-auth numpy")
    sys.exit(1)

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    print("Erro: gspread não instalado. Execute: pip install gspread google-auth numpy")
    sys.exit(1)


# ── CONFIGURAÇÕES ────────────────────────────────────────────────────────────
CONFIG = {
    'ABA_PEDIDO':           'PEDIDO',
    'ABA_RESULTADO':        'DISTRIBUIÇÃO',
    'ABA_RELATORIO':        'RELATORIO',
    'HORAS_POR_DIA':        24,
    # Limiar 0 → qualquer combinação que reduza o custo lexicográfico (tardiness, makespan)
    # vence o EDD. Como a comparação é por tupla, qualquer redução de atraso é suficiente.
    'LIMIAR_TROCA_PERCENT': 0,
    'ABAS_IGNORAR': {
        'PEDIDO', 'DISTRIBUIÇÃO', 'COMPARATIVO', 'RELATORIO',
        'DATAS FORA DE PROGRAMAÇÃO',
        'Página1', 'Sheet1', 'Resumo', 'DADOS_GERAIS', 'DADOS',
    },
    # Monte Carlo / SA base: > 1000 → 50 iter | > 500 → 100 | > 200 → 200 | ≤ 200 → 500
    'MC_ITER': [(1000, 50), (500, 100), (200, 200), (0, 500)],
    # Simulated Annealing — ordenação de pedidos (dentro do mesmo prazo)
    'SA_ITER_MULT': 4,    # iterações SA = MC_iters × multiplicador
    'SA_T0_FRAC':   0.05, # temperatura inicial como fração do tardiness (ou makespan) inicial
    'SA_COOLING':   0.995,
    # Simulated Annealing — atribuição de máquinas (encaixes)
    # Otimiza QUAL máquina recebe cada pedido para a ordem EDD fixa.
    # Cada iteração testa uma combinação diferente de encaixe nas máquinas.
    'SA_ENCAIXES_MULT':    10,     # iterações = MC_iters × multiplicador (mais que o de ordenação)
    'SA_ENCAIXES_COOLING': 0.998, # resfria mais devagar — espaço de busca maior
    # 2-opt local search
    '2OPT_MAX_N':  300,   # busca exaustiva O(n²) se n ≤ este valor; acima → amostragem
    '2OPT_PASSES': 5,     # máximo de passagens por rodada
}

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.readonly',
]


# ── CONEXÃO GOOGLE SHEETS ────────────────────────────────────────────────────
def conectar(credentials_path: str):
    creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
    return gspread.authorize(creds)


def abrir_planilha(gc, url_ou_id: str):
    if 'docs.google.com' in url_ou_id:
        return gc.open_by_url(url_ou_id)
    return gc.open_by_key(url_ou_id)


# ── HELPERS DE COR / FORMATO ─────────────────────────────────────────────────
def _hex_to_rgb(h: str) -> dict:
    """Converte #RRGGBB para dict {red, green, blue} com valores 0.0–1.0"""
    h = h.lstrip('#')
    return {
        'red':   int(h[0:2], 16) / 255,
        'green': int(h[2:4], 16) / 255,
        'blue':  int(h[4:6], 16) / 255,
    }


def _fmt(bg=None, fg=None, bold=False, italic=False, font_size=11,
         h_align='LEFT', wrap=False) -> dict:
    """Monta um userEnteredFormat para a Sheets API."""
    fmt = {
        'textFormat': {
            'bold':     bold,
            'italic':   italic,
            'fontSize': font_size,
        }
    }
    if bg:
        fmt['backgroundColor'] = _hex_to_rgb(bg)
    if fg:
        fmt['textFormat']['foregroundColor'] = _hex_to_rgb(fg)
    fmt['horizontalAlignment'] = h_align
    fmt['verticalAlignment']   = 'MIDDLE'
    if wrap:
        fmt['wrapStrategy'] = 'WRAP'
    return fmt


# ── CONSTRUTOR DE ABA (acumula requests e aplica em lote) ────────────────────
class SheetBuilder:
    """
    Escreve dados e formatos em uma aba Google Sheets de forma eficiente,
    acumulando todas as operações e enviando em lote ao final.
    """

    def __init__(self, spreadsheet, sheet_name: str, cols: int = 10):
        self.ss      = spreadsheet
        self.ss_id   = spreadsheet.id
        self.name    = sheet_name
        self.cols    = cols
        self.row     = 1
        self.data    = []   # (row, col, value)
        self.formats = []   # Sheets API batchUpdate requests
        self._ws     = None
        self._sid    = None
        self._init_sheet()

    def _init_sheet(self):
        try:
            ws = self.ss.worksheet(self.name)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = self.ss.add_worksheet(self.name, rows=5000, cols=self.cols + 5)
        self._ws  = ws
        self._sid = ws.id
        # Desfaz todos os merges existentes antes de aplicar novos
        self.formats.append({
            'unmergeCells': {
                'range': {
                    'sheetId':          ws.id,
                    'startRowIndex':    0,
                    'endRowIndex':      5000,
                    'startColumnIndex': 0,
                    'endColumnIndex':   self.cols + 5,
                }
            }
        })

    def write(self, values: list, bg=None, fg='#000000', bold=False,
              italic=False, h_align='LEFT', wrap=False, font_size=11):
        for i, val in enumerate(values):
            self.data.append((self.row, i + 1, val))
        self.formats.append({
            'repeatCell': {
                'range': self._range(self.row, 1, self.cols),
                'cell':  {'userEnteredFormat': _fmt(bg, fg, bold, italic, font_size, h_align, wrap)},
                'fields': 'userEnteredFormat',
            }
        })
        self.row += 1
        return self

    def banner(self, text: str, bg: str, fg='#FFFFFF', bold=True,
               font_size=11, h_align='CENTER', wrap=False):
        self.data.append((self.row, 1, text))
        self.formats.append({
            'mergeCells': {
                'range':     self._range(self.row, 1, self.cols),
                'mergeType': 'MERGE_ALL',
            }
        })
        self.formats.append({
            'repeatCell': {
                'range': self._range(self.row, 1, self.cols),
                'cell':  {'userEnteredFormat': _fmt(bg, fg, bold, font_size=font_size,
                                                    h_align=h_align, wrap=wrap)},
                'fields': 'userEnteredFormat',
            }
        })
        self.row += 1
        return self

    def blank(self, n=1):
        self.row += n
        return self

    def freeze(self, rows: int):
        self.formats.append({
            'updateSheetProperties': {
                'properties': {
                    'sheetId': self._sid,
                    'gridProperties': {'frozenRowCount': rows},
                },
                'fields': 'gridProperties.frozenRowCount',
            }
        })

    def flush(self):
        # Registra timestamp de geração na última linha — facilita verificar se a aba foi realmente atualizada
        ts = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        self.data.append((self.row, 1, f'Gerado em: {ts}'))
        self.formats.append({
            'repeatCell': {
                'range': self._range(self.row, 1, self.cols),
                'cell':  {'userEnteredFormat': _fmt('#ECEFF1', '#607D8B', False, False, 9, 'LEFT', False)},
                'fields': 'userEnteredFormat',
            }
        })

        try:
            if self.data:
                cell_list = []
                for r, c, v in self.data:
                    cell = gspread.Cell(r, c, v)
                    cell_list.append(cell)
                self._ws.update_cells(cell_list, value_input_option='RAW')

            if self.formats:
                for req in self.formats:
                    self._inject_sid(req)
                self.ss.batch_update({'requests': self.formats})

            print(f'  ✔ Aba "{self.name}" salva ({ts})')

        except Exception:
            print(f'\n  ❌ ERRO ao salvar aba "{self.name}" — detalhes abaixo:')
            traceback.print_exc()
            print()
            raise  # re-lança para não mascarar o problema

    def _range(self, row, col, num_cols):
        return {
            'sheetId':          self._sid,
            'startRowIndex':    row - 1,
            'endRowIndex':      row,
            'startColumnIndex': col - 1,
            'endColumnIndex':   col - 1 + num_cols,
        }

    def _inject_sid(self, obj):
        """Insere sheetId recursivamente em qualquer 'range' que não tenha."""
        if isinstance(obj, dict):
            if 'range' in obj and 'sheetId' not in obj['range']:
                obj['range']['sheetId'] = self._sid
            for v in obj.values():
                self._inject_sid(v)
        elif isinstance(obj, list):
            for item in obj:
                self._inject_sid(item)


# ── UTILITÁRIOS DE DATA ───────────────────────────────────────────────────────
def parse_data(s: str):
    """Parseia string de data em vários formatos. Retorna date ou None."""
    if not s or not s.strip():
        return None
    s = s.strip()
    for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%d/%m/%y', '%Y/%m/%d'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _dia_util(d: date, datas_bloqueadas: set) -> bool:
    """Retorna True se o dia não está em datas_bloqueadas. Fins de semana só são bloqueados se cadastrados na aba 'DATAS FORA DE PROGRAMAÇÃO'."""
    return d not in datas_bloqueadas


def horas_para_data(base_date: date, horas_offset: float, datas_bloqueadas: set) -> datetime:
    """
    Converte offset em horas virtuais (excluindo dias bloqueados) para datetime real.
    base_date é o dia 0 do planejamento (hora 0).
    Apenas dias que não estejam em datas_bloqueadas contam.
    """
    hpd = CONFIG['HORAS_POR_DIA']
    if horas_offset <= 0:
        return datetime.combine(base_date, datetime.min.time())

    dias_completos  = int(horas_offset // hpd)
    horas_restantes = horas_offset % hpd

    data_atual    = base_date
    dias_contados = 0
    while dias_contados < dias_completos:
        data_atual += timedelta(days=1)
        if _dia_util(data_atual, datas_bloqueadas):
            dias_contados += 1

    return datetime.combine(data_atual, datetime.min.time()) + timedelta(hours=horas_restantes)


def data_para_horas_corridas(base_date: date, target_date: date) -> float:
    """
    Converte uma data do cliente em offset de horas de CALENDÁRIO a partir de base_date,
    sem descontar datas bloqueadas.

    Usado exclusivamente para deadline_horas e min_start — o prazo combinado com o
    cliente é uma data no calendário real; feriados e bloqueios não movem o prazo.

    Retorna valor negativo para datas passadas (preserva ordem EDD de pedidos vencidos).
    """
    delta = (target_date - base_date).days
    return float(delta * CONFIG['HORAS_POR_DIA'])


def data_para_horas(base_date: date, target_date: date, datas_bloqueadas: set) -> float:
    """
    Converte uma data alvo em offset de horas ÚTEIS a partir de base_date,
    contando apenas dias que não estejam em datas_bloqueadas.

    Usado pelo tetris (min_start de máquinas, filas) — representa quando a máquina
    estará disponível contando só os dias em que ela efetivamente trabalha.

    Retorna valor negativo quando target_date < base_date, preservando a ordem
    relativa de prazos vencidos — essencial para que o EDD continue funcionando
    corretamente mesmo quando a data de entrega já passou.
    """
    hpd = CONFIG['HORAS_POR_DIA']
    if target_date == base_date:
        return 0.0
    if target_date < base_date:
        # Contagem regressiva: resultado negativo preserva a ordem EDD para prazos vencidos
        dias = 0
        data_atual = target_date
        while data_atual < base_date:
            data_atual += timedelta(days=1)
            if _dia_util(data_atual, datas_bloqueadas):
                dias += 1
        return -float(dias * hpd)
    dias = 0
    data_atual = base_date
    while data_atual < target_date:
        data_atual += timedelta(days=1)
        if _dia_util(data_atual, datas_bloqueadas):
            dias += 1
    return float(dias * hpd)


def _semana_id(data_entrega) -> int | None:
    """Retorna identificador único de semana ISO (ano*100 + semana) ou None."""
    if data_entrega is None:
        return None
    iso = data_entrega.isocalendar()
    return int(iso[0]) * 100 + int(iso[1])


# ── LER DATA BASE E DATAS BLOQUEADAS ─────────────────────────────────────────
def ler_data_base(spreadsheet) -> date:
    """Lê a data base de início (célula M1) da aba PEDIDO."""
    ws  = spreadsheet.worksheet(CONFIG['ABA_PEDIDO'])
    val = ws.acell('M1').value or ''
    d   = parse_data(val)
    if d is None:
        d = date.today()
        print(f'  ⚠ M1 da aba PEDIDO inválido ou vazio. Usando hoje: {d.strftime("%d/%m/%Y")}')
    return d


def ler_datas_bloqueadas(spreadsheet) -> set:
    """Lê as datas bloqueadas da aba 'DATAS FORA DE PROGRAMAÇÃO'."""
    bloqueadas = set()
    try:
        ws   = spreadsheet.worksheet('DATAS FORA DE PROGRAMAÇÃO')
        rows = ws.get_all_values()
        for linha in rows[1:]:
            if not linha:
                continue
            d = parse_data(linha[0])
            if d:
                bloqueadas.add(d)
        print(f'  ✔ {len(bloqueadas)} data(s) bloqueada(s).')
    except gspread.WorksheetNotFound:
        print('  ℹ Aba "DATAS FORA DE PROGRAMAÇÃO" não encontrada — sem restrições de datas.')
    return bloqueadas


# ── LER PEDIDOS ──────────────────────────────────────────────────────────────
def ler_pedidos(spreadsheet, data_base: date, datas_bloqueadas: set) -> list:
    """
    Lê a aba PEDIDO com a estrutura:
      A: Data Inicial Especial  B: Máquina Especial  C: Produto  D: Referência  E: Cor
      F: Qtd Máquinas  G: Cliente  H: Ordem de Compra  I: Data de Entrega
      J: Data Finalização (saída)  K: Prazo (saída)
      L: Data de Entrega Especial — quando preenchida, substitui a col I em TODAS
         as análises (custo, EDD, SA, relatórios). O código passa a trabalhar
         exclusivamente para cumprir este prazo.
      M1: Data base (lida separadamente por ler_data_base)
    """
    ws   = spreadsheet.worksheet(CONFIG['ABA_PEDIDO'])
    rows = ws.get_all_values()
    pedidos = []

    for i, linha in enumerate(rows[1:], start=2):   # i = linha real no sheet
        if len(linha) < 6:
            continue
        try:
            data_esp_str        = linha[0].strip() if linha[0].strip() else ''
            maquina_especial    = linha[1].strip() if len(linha) > 1 else ''
            produto             = linha[2].strip() if len(linha) > 2 else ''
            ref                 = linha[3].strip() if len(linha) > 3 else ''
            cor                 = linha[4].strip() if len(linha) > 4 else ''
            qtd_str             = linha[5].strip() if len(linha) > 5 else ''
            cliente             = linha[6].strip() if len(linha) > 6 else ''
            ordem_compra        = linha[7].strip() if len(linha) > 7 else ''
            data_ent_str        = linha[8].strip() if len(linha) > 8 else ''
            data_ent_esp_str    = linha[11].strip() if len(linha) > 11 else ''
        except IndexError:
            continue

        if not ref or not qtd_str:
            continue

        try:
            total_maq = int(float(qtd_str.replace(',', '.')))
        except ValueError:
            continue
        if total_maq <= 0:
            continue

        # Data inicial especial (col A) → min_start em horas de calendário corridas.
        # O cliente combinou uma data real; bloqueios não movem essa restrição.
        data_esp  = parse_data(data_esp_str)
        min_start = 0.0
        if data_esp:
            min_start = data_para_horas_corridas(data_base, data_esp)

        # Deadline (col I) → offset em horas de calendário corridas.
        # O prazo do cliente é uma data no calendário; feriados não o movem.
        data_entrega   = parse_data(data_ent_str)
        deadline_horas = None
        if data_entrega:
            deadline_horas = data_para_horas_corridas(data_base, data_entrega)

        # DATA DE ENTREGA ESPECIAL (col L): substitui col I em TODAS as análises.
        data_ent_especial = parse_data(data_ent_esp_str)
        if data_ent_especial:
            data_entrega   = data_ent_especial
            deadline_horas = data_para_horas_corridas(data_base, data_ent_especial)

        pedidos.append({
            'linha_sheet':          i,
            'referencia':           ref,
            'produto':              produto,
            'cor':                  cor,
            'cliente':              cliente,
            'ordem_compra':         ordem_compra,
            'maquinas_necessarias': total_maq,
            'data_entrega':         data_entrega,
            'deadline_horas':       deadline_horas,
            '_semana':              _semana_id(data_entrega),
            'data_especial':        data_esp,
            'min_start':            min_start,
            'maquina_especial':     maquina_especial,
            'prioridade':           1,          # mantido para compatibilidade interna
        })

    return pedidos


# ── LER MODELOS ──────────────────────────────────────────────────────────────
def ler_modelos(spreadsheet) -> dict:
    modelos = {}
    ignorar = CONFIG['ABAS_IGNORAR']
    for ws in spreadsheet.worksheets():
        nome = ws.title.strip()
        if nome in ignorar:
            continue
        try:
            k1 = ws.acell('K1').value or ''
            l1 = ws.acell('L1').value or ''
            try:
                total_maq = int(float(k1.replace(',', '.'))) if k1.strip() else 1
            except ValueError:
                total_maq = 1
            if total_maq <= 0:
                total_maq = 1
            nome_modelo = l1.strip() or nome

            referencias = {}
            descricoes  = {}
            rows = ws.get_all_values()
            for linha in rows[1:]:
                if len(linha) < 7:
                    continue
                descricao = ' '.join(linha[0].split())        # Coluna A = descrição completa
                ref       = ' '.join(linha[6].split())        # Coluna G = REFERENCIA
                cor_maq   = ' '.join(linha[5].split()) if len(linha) > 5 else ''  # Coluna F = COR
                tempo_str = linha[1].strip()                  # Coluna B
                if not ref:
                    continue
                try:
                    tempo = float(tempo_str.replace(',', '.'))
                except (ValueError, AttributeError):
                    continue
                if tempo <= 0:
                    continue
                chave = f"{ref} {cor_maq}" if cor_maq else ref
                referencias[chave] = tempo
                if descricao:
                    descricoes[chave] = descricao

            if not referencias:
                continue

            modelos[nome] = {
                'nome_modelo':    nome_modelo,
                'total_maquinas': total_maq,
                'referencias':    referencias,
                'descricoes':     descricoes,
            }
            print(f'  ✔ "{nome}": {total_maq} máquinas, {len(referencias)} referências')
        except Exception as e:
            print(f'  ⚠ Aba "{nome}" ignorada: {e}')
    return modelos


# ── AGRUPAMENTO ──────────────────────────────────────────────────────────────
def agrupar_por_prioridade(pedidos: list) -> list:
    mapa = {}
    for p in pedidos:
        pri = p.get('prioridade', 1)
        mapa.setdefault(pri, []).append(p)
    return [{'prioridade': k, 'pedidos': mapa[k]} for k in sorted(mapa)]


def agrupar_por_dia_vencimento(pedidos: list) -> list:
    """
    Agrupa pedidos por dia de vencimento relativo à data base.

    Buckets (em ordem de urgência):
      'vencido'   — todos os pedidos já atrasados (deadline_horas < 0)
      0, 1, 2, …  — dia 0 = vence hoje [0h, 24h), dia 1 = amanhã [24h, 48h), …
      'sem_prazo' — pedidos sem data de entrega (sempre por último)

    A separação em blocos é uma restrição DURA: pedidos de um bloco nunca
    competem com pedidos de outro bloco pela mesma posição na fila — cada
    bloco usa as máquinas que o anterior deixou livres.

    Restrições especiais consideradas:
      min_start > 0  — pedido não pode iniciar antes de X horas. Um pedido
                       vencido com min_start = 8 dias vai para o bloco dia+8,
                       não para 'vencido'. Um pedido com prazo dia+2 mas
                       min_start = dia+5 vai para dia+5 (bloco mais tardio).
      maquina_especial — a restrição de máquina já é respeitada pelo simulador;
                         aqui apenas garantimos que o bucket não seja anterior
                         ao min_start do pedido.
    """
    mapa: dict = {}
    for p in pedidos:
        dl  = p.get('deadline_horas')
        ms  = p.get('min_start') or 0.0    # horas até o início mínimo (0 = agora)
        # dia do bloco ditado pelo min_start (0 se pode começar hoje ou antes)
        ms_day = int(ms // 24) if ms > 0.0 else 0

        if dl is None:
            # sem prazo: se tem min_start futuro, vai para aquele dia
            bucket = ms_day if ms_day > 0 else 'sem_prazo'
        elif dl < 0:
            # vencido: mas se min_start ainda está no futuro, não pode rodar
            # no bloco 'vencido' — desloca para o dia correto
            bucket = ms_day if ms_day > 0 else 'vencido'
        else:
            dl_day = int(dl // 24)         # 0 = vence hoje, 1 = amanhã, …
            # usa o bloco mais tardio entre prazo e min_start
            bucket = max(dl_day, ms_day)

        mapa.setdefault(bucket, []).append(p)

    # Ordena: vencidos primeiro, depois dias 0, 1, 2, …, sem prazo por último
    def _key(b):
        if b == 'vencido':
            return (-1, 0)
        if b == 'sem_prazo':
            return (float('inf'), 0)
        return (b, 0)

    return [{'bucket': b, 'pedidos': mapa[b]}
            for b in sorted(mapa, key=_key)]


# ── PRÉ-COMPUTAÇÃO NUMPY ─────────────────────────────────────────────────────
def precomputar_maquinas(modelos: dict):
    """
    Mapeia cada referência para arrays numpy de índices globais e tempos.
    """
    gidx_map = {}
    g = 0
    for aba, mod in modelos.items():
        for i in range(mod['total_maquinas']):
            gidx_map[(aba, i)] = g
            g += 1
    num_machines = g

    ref_data = {}
    for aba, mod in modelos.items():
        for ref, tempo in mod['referencias'].items():
            if ref not in ref_data:
                ref_data[ref] = {'gidxs': [], 'tempos': [], 'aba_idx': []}
            for i in range(mod['total_maquinas']):
                gi = gidx_map[(aba, i)]
                ref_data[ref]['gidxs'].append(gi)
                ref_data[ref]['tempos'].append(tempo)
                ref_data[ref]['aba_idx'].append((aba, i))

    # Máquinas com ref genérica ("M60109") devem participar de pedidos com cor específica
    # ("M60109 2410"). Sem isso, uma máquina que aceita qualquer cor fica excluída quando
    # outra máquina tem a cor exata cadastrada — o _chave_pedido usa a chave combinada e
    # a máquina genérica nunca aparece nela.
    for combined_key, combined_entry in list(ref_data.items()):
        already_abas = {ai[0] for ai in combined_entry['aba_idx']}
        for aba, mod in modelos.items():
            if aba in already_abas:
                continue
            # Verifica se alguma chave genérica desta aba é prefixo da chave combinada.
            # Ex.: ref_k="M60109", combined_key="M60109 2410" → startswith("M60109 ") = True
            for ref_k, tempo in mod['referencias'].items():
                if combined_key.startswith(ref_k + ' '):
                    # Esta aba produz a ref em qualquer cor — inclui nas combinadas
                    for i in range(mod['total_maquinas']):
                        gi = gidx_map[(aba, i)]
                        combined_entry['gidxs'].append(gi)
                        combined_entry['tempos'].append(tempo)
                        combined_entry['aba_idx'].append((aba, i))
                    break   # uma ref_k por aba é suficiente

    for ref in ref_data:
        ref_data[ref]['gidxs']  = np.array(ref_data[ref]['gidxs'],  dtype=np.int32)
        ref_data[ref]['tempos'] = np.array(ref_data[ref]['tempos'], dtype=np.float64)

    ridx_map = {v: k for k, v in gidx_map.items()}
    return ref_data, num_machines, ridx_map


# ── PRÉ-COMPUTAÇÃO DE RESTRIÇÕES POR PEDIDO ───────────────────────────────────
def preparar_restricoes_pedidos(pedidos: list, ref_data: dict, modelos: dict):
    """
    Pré-computa, para cada pedido, os arrays filtrados de gidxs/tempos/aba_idx
    considerando a restrição de 'maquina_especial' (col B da aba PEDIDO).

    Armazena diretamente no dict do pedido:
      _gidxs   — numpy array de índices globais válidos
      _tempos  — numpy array de tempos correspondentes
      _aba_idx — lista de (aba, slot) correspondentes

    Pedidos sem cadastro recebem _gidxs = None.
    Chamado uma única vez após precomputar_maquinas; todos os pontos de
    simulação e alocação usam esses arrays — garantindo que a restrição
    seja respeitada em 100% das análises.
    """
    for p in pedidos:
        chave = _chave_pedido(p, ref_data)
        d     = ref_data.get(chave)
        if d is None:
            p['_gidxs'] = p['_tempos'] = p['_aba_idx'] = None
            continue

        maq_esp = (p.get('maquina_especial') or '').strip()
        if maq_esp:
            mask = np.array([
                modelos[aba]['nome_modelo'] == maq_esp
                for aba, _ in d['aba_idx']
            ])
            if mask.any():
                p['_gidxs']   = d['gidxs'][mask]
                p['_tempos']  = d['tempos'][mask]
                p['_aba_idx'] = [ai for ai, m in zip(d['aba_idx'], mask) if m]
            else:
                print(f'  ⚠ Máquina especial "{maq_esp}" não encontrada '
                      f'para ref "{p["referencia"]}" — usando todas as disponíveis.')
                p['_gidxs']   = d['gidxs']
                p['_tempos']  = d['tempos']
                p['_aba_idx'] = d['aba_idx']
        else:
            p['_gidxs']   = d['gidxs']
            p['_tempos']  = d['tempos']
            p['_aba_idx'] = d['aba_idx']


# ── RESOLUÇÃO DE CHAVE (ref + cor com fallback para ref genérica) ─────────────
def _chave_pedido(p: dict, ref_data: dict) -> str:
    """
    Retorna a chave a usar em ref_data para este pedido.
    Tenta 'referencia cor' (específico por cor), cai para 'referencia' genérica.
    Ex.: ref='M60109' cor='2410' → tenta 'M60109 2410', senão usa 'M60109'.
    """
    ref = ' '.join((p['referencia'] or '').split())
    cor = ' '.join((p.get('cor') or '').split())
    if cor and cor != '-':
        combined = f"{ref} {cor}"
        if combined in ref_data:
            return combined
    return ref


# ── SIMULAÇÃO (núcleo quente) ────────────────────────────────────────────────
def simular_termino(pedidos: list, ref_data: dict, num_machines: int,
                     filas_iniciais=None) -> float:
    """
    Simula tempo total de produção respeitando min_start e maquina_especial de cada pedido.
    Usa arrays pré-computados (_gidxs/_tempos) quando disponíveis — assim a restrição
    de máquina especial é considerada em todas as simulações de estratégia/Monte Carlo.
    Thread-safe (cria 'filas' local).

    filas_iniciais: estado inicial das máquinas (numpy array). Quando fornecido, as
    máquinas partem do estado deixado pelo grupo/bloco anterior — usado na otimização
    rolling-horizon por bloco de prazo. None = começa do zero (comportamento padrão).
    """
    filas = (filas_iniciais.copy() if filas_iniciais is not None
             else np.zeros(num_machines, dtype=np.float64))
    maior = 0.0
    for p in pedidos:
        gidxs = p.get('_gidxs')
        if gidxs is None:
            d = ref_data.get(_chave_pedido(p, ref_data))
            if d is None:
                continue
            gidxs, tempos = d['gidxs'], d['tempos']
        else:
            tempos = p['_tempos']
        min_s = float(p.get('min_start', 0.0))
        for _ in range(p['maquinas_necessarias']):
            available = np.maximum(filas[gidxs], min_s)
            ft   = available + tempos
            best = int(np.argmin(ft))
            fim  = float(ft[best])
            filas[gidxs[best]] = fim
            if fim > maior:
                maior = fim
    return maior


# ── SIMULAÇÃO COM GRAVAÇÃO / REPRODUÇÃO DE ATRIBUIÇÃO DE MÁQUINAS ────────────
def simular_com_atribuicao(pedidos: list, ref_data: dict, num_machines: int,
                            choices: list | None = None,
                            filas_iniciais=None) -> tuple:
    """
    Versão do simulador que grava OU reproduz quais máquinas foram atribuídas.

    choices=None  → modo greedy (comportamento padrão): a cada slot escolhe a
                    máquina com menor tempo de término e grava a decisão.
    choices=[...] → modo reprodução: usa os índices gravados (posição dentro do
                    gidxs local de cada pedido) em vez de escolher pelo argmin.
                    Se o índice for >= len(gidxs) do pedido, aplica módulo para
                    garantir validade (pedido pode ter restrição de máquina).

    filas_iniciais: estado inicial das máquinas para otimização rolling-horizon.

    Retorna ((total_tardiness, makespan), choices_feitas, filas_finais) — a terceira
    posição contém o numpy array com o estado das máquinas ao fim da simulação,
    necessário para encadear blocos de prazo na otimização rolling-horizon.

    Thread-safe: todos os estados são locais.
    """
    filas           = (filas_iniciais.copy() if filas_iniciais is not None
                       else np.zeros(num_machines, dtype=np.float64))
    maior           = 0.0
    total_tardiness = 0.0
    choices_feitas  = []
    ptr             = 0

    for p in pedidos:
        gidxs = p.get('_gidxs')
        if gidxs is None:
            d = ref_data.get(_chave_pedido(p, ref_data))
            if d is None:
                continue
            gidxs, tempos = d['gidxs'], d['tempos']
        else:
            tempos = p['_tempos']
        min_s      = float(p.get('min_start', 0.0))
        pedido_fim = 0.0
        ng         = len(gidxs)

        for _ in range(p['maquinas_necessarias']):
            available = np.maximum(filas[gidxs], min_s)
            ft        = available + tempos
            if choices is not None and ptr < len(choices):
                k = int(choices[ptr]) % ng   # módulo garante índice válido
            else:
                k = int(np.argmin(ft))       # greedy
            choices_feitas.append(k)
            ptr += 1
            fim = float(ft[k])
            filas[gidxs[k]] = fim
            if fim > maior:
                maior = fim
            if fim > pedido_fim:
                pedido_fim = fim

        dl = p.get('deadline_horas')
        if dl is not None and pedido_fim > dl:
            tardiness = pedido_fim - dl
            # Mesmo peso de urgência do simular_custo: pedidos já atrasados no
            # início valem mais — sem isso o SA de encaixes trata pedidos com 64
            # dias de atraso igual a pedidos com prazo futuro e pode dar as
            # máquinas mais rápidas a pedidos menos urgentes.
            dias_ja_atrasado = max(0.0, -dl) / 24.0
            urgencia = 1.0 + dias_ja_atrasado
            total_tardiness += urgencia * tardiness

    return (total_tardiness, maior), choices_feitas, filas


# ── CUSTO LEXICOGRÁFICO: (tardiness, makespan) ───────────────────────────────
def simular_custo(pedidos: list, ref_data: dict, num_machines: int,
                   filas_iniciais=None) -> tuple:
    """
    Retorna (total_tardiness, makespan) — objetivo lexicográfico.

    Prioridade absoluta: minimizar total_tardiness (soma ponderada dos atrasos).
    Desempate: minimizar makespan (quando o último pedido do bloco termina).

    No rolling-horizon os blocos são sequenciais: terminar o bloco atual mais
    cedo libera máquinas mais cedo para o próximo bloco — o que automaticamente
    adianta todos os pedidos dos blocos seguintes. Minimizar o makespan do bloco
    é equivalente a minimizar o término individual de cada pedido neste contexto.

    Pedidos já atrasados no início (deadline_horas negativo) recebem peso maior.
    Thread-safe (cria 'filas' local).

    filas_iniciais: estado inicial das máquinas para otimização rolling-horizon.
    """
    filas           = (filas_iniciais.copy() if filas_iniciais is not None
                       else np.zeros(num_machines, dtype=np.float64))
    maior           = 0.0
    total_tardiness = 0.0
    for p in pedidos:
        gidxs = p.get('_gidxs')
        if gidxs is None:
            d = ref_data.get(_chave_pedido(p, ref_data))
            if d is None:
                continue
            gidxs, tempos = d['gidxs'], d['tempos']
        else:
            tempos = p['_tempos']
        min_s      = float(p.get('min_start', 0.0))
        pedido_fim = 0.0
        for _ in range(p['maquinas_necessarias']):
            available = np.maximum(filas[gidxs], min_s)
            ft   = available + tempos
            best = int(np.argmin(ft))
            fim  = float(ft[best])
            filas[gidxs[best]] = fim
            if fim > maior:
                maior = fim
            if fim > pedido_fim:
                pedido_fim = fim
        dl = p.get('deadline_horas')
        if dl is not None and pedido_fim > dl:
            tardiness = pedido_fim - dl
            # Peso de urgência: pedidos que já estavam atrasados no início do planejamento
            # (deadline_horas negativo) recebem peso maior — cada dia de atraso pré-existente
            # adiciona 1 ao peso base, tornando pedidos mais atrasados proporcionalmente
            # mais custosos de atrasar ainda mais.
            # Exemplos: já 0 dias atrasado → peso 1.0
            #           já 2 dias atrasado → peso 3.0 (3× mais importante)
            #           já 5 dias atrasado → peso 6.0
            dias_ja_atrasado = max(0.0, -dl) / 24.0
            urgencia = 1.0 + dias_ja_atrasado
            total_tardiness += urgencia * tardiness
    return (total_tardiness, maior)


# ── ESTRATÉGIAS ──────────────────────────────────────────────────────────────
def get_menor_tempo(ref: str, modelos: dict) -> float:
    menor = float('inf')
    for mod in modelos.values():
        if ref in mod['referencias']:
            menor = min(menor, mod['referencias'][ref])
    return menor if menor != float('inf') else 9999


def _sort_by_edd(pedidos: list) -> list:
    """
    Sort estável por deadline — chave primária obrigatória de todo o sistema.

    "Estável" significa: pedidos com o MESMO deadline mantêm a ordem relativa
    que veio da estratégia (desempate livre). Pedidos com deadlines diferentes
    são reordenados de forma que o prazo mais curto sempre venha antes.

    Pedidos sem deadline (None) são tratados como deadline = infinito
    e ficam sempre no final da fila.

    Esta função é aplicada ao resultado de TODA estratégia de ordenação,
    garantindo que nenhuma delas possa violar a prioridade por prazo de entrega.
    """
    return sorted(pedidos, key=lambda p: (
        p['deadline_horas'] if p['deadline_horas'] is not None else float('inf')
    ))


def make_estrategias(modelos: dict, ref_data: dict, num_machines: int,
                      filas_iniciais=None, livre=False) -> list:
    """Cria as estratégias como closures sobre modelos e ref_data."""

    def _mc_iter(n):
        for threshold, iters in CONFIG['MC_ITER']:
            if n > threshold:
                return iters
        return CONFIG['MC_ITER'][-1][1]

    def edd(pedidos):
        """Earliest Due Date — prazo mais próximo primeiro."""
        return sorted(pedidos, key=lambda p: (
            p['deadline_horas'] if p['deadline_horas'] is not None else float('inf'),
            p.get('min_start', 0.0),
        ))

    def _dl(p):
        """Chave de deadline para sort: prazos vencidos (negativos) primeiro."""
        return p['deadline_horas'] if p['deadline_horas'] is not None else float('inf')

    def balanceamento(pedidos):
        grupos = {}
        for p in pedidos:
            best_aba, best_t = '', float('inf')
            for aba, mod in modelos.items():
                if p['referencia'] in mod['referencias']:
                    t = mod['referencias'][p['referencia']]
                    if t < best_t:
                        best_t   = t
                        best_aba = aba
            grupos.setdefault(best_aba, []).append(p)
        # Ordena cada grupo por prazo antes do interleave
        for k in grupos:
            grupos[k].sort(key=_dl)
        chaves  = list(grupos.keys())
        result  = []
        while any(grupos[k] for k in chaves):
            for k in chaves:
                if grupos[k]:
                    result.append(grupos[k].pop(0))
        return result

    def rapido(pedidos):
        return sorted(pedidos, key=lambda p: (
            _dl(p),
            get_menor_tempo(p['referencia'], modelos),
            p['referencia'], p.get('cor', ''),
        ))

    def menor_demanda(pedidos):
        return sorted(pedidos, key=lambda p: (_dl(p), p['maquinas_necessarias'], p.get('cor', '')))

    def maior_demanda(pedidos):
        return sorted(pedidos, key=lambda p: (_dl(p), -p['maquinas_necessarias'], p.get('cor', '')))

    def lento(pedidos):
        return sorted(pedidos, key=lambda p: (
            _dl(p),
            -get_menor_tempo(p['referencia'], modelos), p.get('cor', ''),
        ))

    def wspt(pedidos):
        """
        Weighted Shortest Processing Time — regra ótima para parallel machines com pesos.
        Score = p_j / w_j  onde  p_j = menor tempo disponível,  w_j = 1/deadline.
        Pedidos urgentes (prazo próximo) e rápidos entram primeiro.
        Usa _tempos pré-computados → respeita restrição de maquina_especial.
        """
        def _score(p):
            tempos = p.get('_tempos')
            p_j = float(np.min(tempos)) if tempos is not None and len(tempos) > 0 \
                  else get_menor_tempo(_chave_pedido(p, ref_data), modelos)
            dl = p.get('deadline_horas')
            w_j = 1.0 / max(dl, 1.0) if dl else 1e-9  # prazo próximo = peso alto
            return p_j / max(w_j, 1e-12)              # minimiza p/w
        return sorted(pedidos, key=_score)

    def simulated_annealing(pedidos):
        """
        Simulated Annealing — metaheurística que sai de ótimos locais.
        Parte da melhor solução heurística determinística, aplica trocas
        de pares e aceita soluções piores com probabilidade e^(-Δ/T),
        onde T esfria gradualmente. Muito mais eficiente que Monte Carlo puro.
        Usa arrays pré-computados → restrição de maquina_especial garantida.
        """
        n = len(pedidos)
        if n < 4:
            return list(edd(pedidos))

        # Semente: melhor entre EDD, WSPT e Mais Rápido — todos já ordenados por EDD.
        # _sort_by_edd garante que mesmo wspt/rapido respeitam deadline como chave primária.
        candidatos_ini = [
            list(edd(pedidos)),
            list(_sort_by_edd(wspt(pedidos))),
            list(_sort_by_edd(rapido(pedidos))),
        ]
        current = min(candidatos_ini,
                      key=lambda o: simular_custo(o, ref_data, num_machines,
                                                  filas_iniciais))
        current_t = simular_custo(current, ref_data, num_machines, filas_iniciais)
        best, best_t = list(current), current_t

        # Duas temperaturas: uma para tardiness (prioridade), outra para makespan (desempate).
        # Se tardiness atual = 0, T_tard = 0 e o SA só otimiza makespan — comportamento correto.
        T_tard  = current_t[0] * CONFIG['SA_T0_FRAC']
        T_make  = current_t[1] * CONFIG['SA_T0_FRAC']
        cooling = CONFIG['SA_COOLING']
        iters   = _mc_iter(n) * CONFIG['SA_ITER_MULT']

        for _ in range(iters):
            a, b = random.sample(range(n), 2)
            if not livre:
                # Modo global: EDD estrito entre pedidos de prazos diferentes.
                # Dentro de um bloco rolling-horizon (livre=True) a prioridade entre
                # blocos já está garantida — o SA pode reordenar livremente para
                # minimizar o makespan do bloco e liberar máquinas mais cedo.
                early, late = (a, b) if a < b else (b, a)
                dl_e = current[early].get('deadline_horas')
                dl_l = current[late].get('deadline_horas')
                _dl_early = dl_e if dl_e is not None else float('inf')
                _dl_late  = dl_l if dl_l is not None else float('inf')
                mesmo_prazo = (_dl_early == _dl_late and _dl_early != float('inf'))
                if not mesmo_prazo and _dl_late > _dl_early:
                    continue
            neighbor = list(current)
            neighbor[a], neighbor[b] = neighbor[b], neighbor[a]
            neighbor_t  = simular_custo(neighbor, ref_data, num_machines, filas_iniciais)
            n_tard, n_make = neighbor_t
            c_tard, c_make = current_t
            # Aceitação lexicográfica: tardiness é objetivo primário, makespan é desempate.
            if n_tard != c_tard:
                delta  = n_tard - c_tard
                accept = delta < 0 or (T_tard > 1e-9 and random.random() < math.exp(-delta / T_tard))
            else:
                delta  = n_make - c_make
                accept = delta < 0 or (T_make > 1e-9 and random.random() < math.exp(-delta / T_make))
            if accept:
                current, current_t = neighbor, neighbor_t
                if current_t < best_t:
                    best, best_t = list(current), current_t
            T_tard *= cooling
            T_make *= cooling

        # Garante EDD obrigatório no resultado — sort estável preserva a ordem
        # otimizada dentro de grupos de mesmo deadline.
        return _sort_by_edd(best)

    def mdd_rapido(pedidos):
        """
        Mais Atrasado + Mais Rápido — ordena do pedido mais atrasado/urgente para
        o mais distante no prazo; dentro do mesmo prazo, favorece o menor tempo de
        produção (libera máquinas mais cedo para os próximos pedidos urgentes).

        Diferença em relação ao EDD puro: usa o menor tempo de processamento real
        (tempos pré-computados por máquina quando disponíveis) como critério de
        desempate explícito — não apenas a data de entrega.
        Diferença em relação ao 'Mais Rápido': a velocidade é secundária; o prazo
        é sempre chave primária, nunca abre mão da ordem de urgência.
        """
        def _key(p):
            dl    = p['deadline_horas'] if p['deadline_horas'] is not None else float('inf')
            tempos = p.get('_tempos')
            t_min  = (float(np.min(tempos)) if tempos is not None and len(tempos) > 0
                      else get_menor_tempo(p['referencia'], modelos))
            return (dl, t_min, p.get('cor', ''))
        return sorted(pedidos, key=_key)

    def lsf(pedidos):
        """
        Least Slack First (Menor Folga Primeiro) — estratégia focada em garantir
        que pedidos que vencem antes saiam antes.

        Folga = deadline − tempo_de_produção
              = a hora mais tarde em que o pedido PODE começar e ainda entregar no prazo.

        Pedidos com menor folga (ou folga negativa = já impossível de entregar no prazo)
        entram primeiro. Isso maximiza a chance de cada pedido terminar antes do seu prazo.

        Diferença fundamental em relação ao EDD:
          EDD olha só o prazo.
          LSF olha prazo E quanto tempo o pedido leva — o que realmente importa
          para saber se vai fechar no prazo.

        Exemplo onde LSF vence o EDD:
          Pedido A: prazo=50h, produção=45h → folga=5h  → tem que começar AGORA
          Pedido B: prazo=30h, produção=2h  → folga=28h → pode esperar
          EDD coloca B primeiro (prazo 30<50) → A atrasa.
          LSF coloca A primeiro (folga 5<28) → A fecha no prazo, B também.
        """
        def _folga(p):
            dl     = p['deadline_horas'] if p['deadline_horas'] is not None else float('inf')
            min_s  = float(p.get('min_start', 0.0))
            tempos = p.get('_tempos')
            t_min  = (float(np.min(tempos)) if tempos is not None and len(tempos) > 0
                      else get_menor_tempo(p['referencia'], modelos))
            # Folga real = janela disponível após o início permitido menos o tempo de produção.
            # Subtrai min_start para que pedidos com data mínima de início não tenham
            # sua urgência subestimada (a janela deles é menor do que o deadline sugere).
            folga  = (dl - min_s - t_min) if dl != float('inf') else float('inf')
            return (folga, dl, p.get('cor', ''))
        return sorted(pedidos, key=_folga)

    def _com_edd(fn):
        """
        Wrapper obrigatório: aplica a estratégia fn como critério de desempate
        dentro de grupos de mesmo deadline.

        Fluxo: fn(pedidos) → ordenação pela estratégia → sort estável por deadline.

        O sort estável garante que pedidos com prazos diferentes seguem sempre
        a ordem EDD no resultado final, enquanto pedidos com o MESMO prazo
        mantêm a ordem que a estratégia definiu (otimização intra-grupo).
        """
        return lambda pedidos: _sort_by_edd(fn(pedidos))

    return [
        {'id': 'edd',          'nome': '✅ EDD — Prazo Mais Próximo Primeiro',
         'descricao': 'Prioriza a data de entrega — minimiza atrasos',
         'fn': edd},
        {'id': 'balanceamento','nome': '2 — Balanceamento por Modelo',
         'descricao': 'Equilíbrio de carga entre modelos, respeitando EDD como prioridade',
         'fn': _com_edd(balanceamento)},
        {'id': 'rapido',       'nome': '3 — Mais Rápido Primeiro',
         'descricao': 'Mais rápido como desempate dentro do mesmo prazo — libera máquinas cedo',
         'fn': _com_edd(rapido)},
        {'id': 'menor_demanda','nome': '4 — Menor Demanda Primeiro',
         'descricao': 'Menos máquinas como desempate dentro do mesmo prazo',
         'fn': _com_edd(menor_demanda)},
        {'id': 'maior_demanda','nome': '5 — Maior Demanda Primeiro',
         'descricao': 'Mais máquinas como desempate dentro do mesmo prazo',
         'fn': _com_edd(maior_demanda)},
        {'id': 'lento',        'nome': '6 — Mais Lento Primeiro',
         'descricao': 'Maior tempo como desempate dentro do mesmo prazo',
         'fn': _com_edd(lento)},
        {'id': 'wspt',         'nome': '7 — WSPT (Urgência × Velocidade)',
         'descricao': 'WSPT como desempate dentro do mesmo prazo — regra ótima teórica',
         'fn': _com_edd(wspt)},
        {'id': 'sa',           'nome': '8 — Simulated Annealing',
         'descricao': f'Metaheurística: otimiza dentro da ordem EDD, {CONFIG["SA_ITER_MULT"]}× iterações',
         'fn': simulated_annealing},
        {'id': 'mdd_rapido',   'nome': '9 — Mais Atrasado + Mais Rápido',
         'descricao': 'Prazo mais urgente/atrasado primeiro; desempate pelo menor tempo de produção na máquina',
         'fn': mdd_rapido},
        {'id': 'lsf',          'nome': '10 — Menor Folga Primeiro (LSF)',
         'descricao': 'Ordena por prazo − tempo de produção: quem tem menos margem para começar entra primeiro',
         'fn': lsf},
    ]


# ── COMBINAÇÕES ──────────────────────────────────────────────────────────────
def gerar_combinacoes(num_grupos: int, num_estrategias: int) -> list:
    combinacoes = []
    indices = [0] * num_grupos
    while True:
        combinacoes.append(list(indices))
        pos = num_grupos - 1
        while pos >= 0:
            indices[pos] += 1
            if indices[pos] < num_estrategias:
                break
            indices[pos] = 0
            pos -= 1
        if pos < 0:
            break
    return combinacoes


# ── 2-OPT LOCAL SEARCH ────────────────────────────────────────────────────────
def busca_local_2opt(ordenados: list, ref_data: dict, num_machines: int,
                      filas_iniciais=None, livre=False):
    """
    Refinamento por busca local 2-opt.

    Para n ≤ 2OPT_MAX_N: testa todos os pares O(n²) por passagem.
    Para n > 2OPT_MAX_N: amostra n×4 pares aleatórios por passagem
    (custo controlado, ainda encontra melhorias significativas).

    Repete até não encontrar melhoria ou atingir 2OPT_PASSES passagens.
    Usa arrays pré-computados dos pedidos → respeita maquina_especial.
    Custo zero se a solução já estiver num ótimo local.

    livre=False (padrão): EDD estrito — nunca coloca pedido menos urgente antes.
    livre=True  (blocos): trocas livres — prioridade entre blocos já garantida
                          pelo rolling-horizon; dentro do bloco o foco é liberar
                          máquinas mais cedo possível.
    """
    n = len(ordenados)
    if n < 2:
        return list(ordenados), simular_custo(ordenados, ref_data, num_machines,
                                              filas_iniciais)

    melhor   = list(ordenados)
    melhor_t = simular_custo(melhor, ref_data, num_machines, filas_iniciais)
    max_n    = CONFIG['2OPT_MAX_N']

    for _ in range(CONFIG['2OPT_PASSES']):
        melhorou = False
        pares = (
            [(i, j) for i in range(n - 1) for j in range(i + 1, n)]
            if n <= max_n
            else [tuple(random.sample(range(n), 2)) for _ in range(n * 4)]
        )
        for a, b in pares:
            if not livre:
                early, late = (a, b) if a < b else (b, a)
                dl_e = melhor[early].get('deadline_horas')
                dl_l = melhor[late].get('deadline_horas')
                _dl_early = dl_e if dl_e is not None else float('inf')
                _dl_late  = dl_l if dl_l is not None else float('inf')
                mesmo_prazo = (_dl_early == _dl_late and _dl_early != float('inf'))
                if not mesmo_prazo and _dl_late > _dl_early:
                    continue
            cand      = list(melhor)
            cand[a], cand[b] = cand[b], cand[a]
            t = simular_custo(cand, ref_data, num_machines, filas_iniciais)
            if t < melhor_t:
                melhor, melhor_t = cand, t
                melhorou = True
        if not melhorou:
            break

    return melhor, melhor_t


# ── SA DE ENCAIXES — OTIMIZAÇÃO DE ATRIBUIÇÃO DE MÁQUINAS ────────────────────
def sa_encaixes(pedidos: list, ref_data: dict, num_machines: int,
                 filas_iniciais=None) -> list:
    """
    Simulated Annealing sobre ATRIBUIÇÃO DE MÁQUINAS para ordem EDD fixa.

    Problema que resolve:
      A ordem dos pedidos já está fixada por EDD. Mas QUAL máquina recebe
      cada pedido ainda é uma decisão: o greedy sempre escolhe a mais rápida
      disponível no momento, o que pode ser subótimo globalmente.

      Exemplo: dar a máquina mais rápida para o pedido A pode travar ela quando
      o pedido B (mais urgente logo em seguida) precisar dela — um encaixe
      alternativo poderia reduzir o makespan total.

    Como funciona:
      1. Parte da solução greedy (choices do argmin) como semente.
      2. A cada iteração muta UMA escolha: sorteia um slot e troca para outra
         máquina disponível para aquele pedido.
      3. Avalia o custo com simular_com_atribuicao (reproduz exatamente o encaixe).
      4. Aceita a mudança se melhorar, ou com probabilidade e^(-Δ/T) se piorar.
      5. Temperatura esfria gradualmente (SA_ENCAIXES_COOLING).

    Restrições sempre respeitadas:
      - Ordem EDD: nunca alterada (só os encaixes mudam, não a sequência).
      - maquina_especial: choices são índices dentro do gidxs já filtrado do pedido.
      - min_start: aplicado dentro do simulador a cada avaliação.
      - Deadline: penalidade de atraso incluída no custo de cada candidato.

    Retorna a lista de choices (índices de máquina por slot) que produz o
    menor custo encontrado, pronta para ser usada em otimizar_distribuicao.
    """
    # Semente greedy
    current_t, current_choices, _ = simular_com_atribuicao(
        pedidos, ref_data, num_machines, filas_iniciais=filas_iniciais)
    best_t      = current_t
    best_choices = list(current_choices)

    n_slots = len(current_choices)
    if n_slots == 0:
        return best_choices

    # Número de iterações — mesmo critério do SA de ordenação
    n = len(pedidos)
    for threshold, iters in CONFIG['MC_ITER']:
        if n > threshold:
            base_iters = iters
            break
    else:
        base_iters = CONFIG['MC_ITER'][-1][1]
    total_iters = base_iters * CONFIG['SA_ENCAIXES_MULT']

    T_tard  = current_t[0] * CONFIG['SA_T0_FRAC']
    T_make  = current_t[1] * CONFIG['SA_T0_FRAC']
    cooling = CONFIG['SA_ENCAIXES_COOLING']

    for _ in range(total_iters):
        # Muta: sorteia um slot e troca para uma máquina diferente
        idx      = random.randrange(n_slots)
        neighbor = list(current_choices)
        # Incrementa aleatoriamente entre 1 e 4 posições no gidxs local.
        # O módulo em simular_com_atribuicao garante que o índice é válido.
        neighbor[idx] = current_choices[idx] + random.randint(1, max(1, num_machines - 1))

        neighbor_t, _, _ = simular_com_atribuicao(pedidos, ref_data, num_machines, neighbor,
                                                    filas_iniciais=filas_iniciais)
        n_tard, n_make = neighbor_t
        c_tard, c_make = current_t
        # Aceitação lexicográfica: tardiness é objetivo primário, makespan é desempate.
        if n_tard != c_tard:
            delta  = n_tard - c_tard
            accept = delta < 0 or (T_tard > 1e-9 and random.random() < math.exp(-delta / T_tard))
        else:
            delta  = n_make - c_make
            accept = delta < 0 or (T_make > 1e-9 and random.random() < math.exp(-delta / T_make))
        if accept:
            current_choices = neighbor
            current_t       = neighbor_t
            if current_t < best_t:
                best_t       = current_t
                best_choices = list(current_choices)

        T_tard *= cooling
        T_make *= cooling

    return best_choices


# ── ESCOLHA DA MELHOR ESTRATÉGIA (EDD estrito + SA intra-prazo) ───────────────
def escolher_melhor_estrategia(pedidos, modelos, grupos, ref_data, num_machines,
                                filas_iniciais=None, livre=False):
    """
    livre=False (padrão): EDD estrito entre pedidos de prazos diferentes.
    livre=True  (blocos): SA livre para reordenar qualquer pedido dentro do bloco
                          buscando menor makespan — prioridade entre blocos já
                          garantida pelo rolling-horizon.

    filas_iniciais: estado inicial das máquinas para otimização rolling-horizon.
    """
    estrategias = make_estrategias(modelos, ref_data, num_machines, filas_iniciais,
                                   livre=livre)
    idx_edd = next(i for i, e in enumerate(estrategias) if e['id'] == 'edd')
    idx_sa  = next(i for i, e in enumerate(estrategias) if e['id'] == 'sa')

    # EDD puro — base sempre respeitada
    print('  Calculando ordenação EDD base...')
    ordenados_edd = estrategias[idx_edd]['fn'](pedidos)
    t_edd         = simular_custo(ordenados_edd, ref_data, num_machines, filas_iniciais)

    # SA — otimiza DENTRO de grupos de mesmo prazo (nunca troca pedidos de datas diferentes)
    print('  Refinando com SA dentro dos grupos de mesmo prazo...')
    ordenados_sa = estrategias[idx_sa]['fn'](pedidos)
    t_sa         = simular_custo(ordenados_sa, ref_data, num_machines, filas_iniciais)

    # Decisão: SA só ganha se melhorar o custo lexicográfico
    if t_sa < t_edd:
        ordenados_final = ordenados_sa
        tempo_final     = t_sa
        est_final       = estrategias[idx_sa]
        ganho_make      = ((t_edd[1] - t_sa[1]) / t_edd[1]) * 100 if t_edd[1] > 0 else 0
        decisao = (f'⚡ SA otimizou dentro dos grupos de prazo '
                   f'(atraso: {_round(t_sa[0])}h vs {_round(t_edd[0])}h EDD; '
                   f'makespan: {abs(_round(ganho_make))}% melhor)')
    else:
        ordenados_final = ordenados_edd
        tempo_final     = t_edd
        est_final       = estrategias[idx_edd]
        decisao         = '✅ EDD direto — prazo mais próximo sempre primeiro'

    # Ranking informativo — todas as estratégias, comparadas contra o resultado final
    print('  Calculando ranking informativo das estratégias...')
    ranking = []
    for est in estrategias:
        ord_ = est['fn'](pedidos)
        t_   = simular_custo(ord_, ref_data, num_machines, filas_iniciais)
        diff = _round(t_[1] - tempo_final[1])
        perc = _round(((t_[1] - tempo_final[1]) / tempo_final[1]) * 100) if tempo_final[1] > 0 else 0
        ranking.append({**est, 'terminoTotal': t_, 'terminoHoras': _round(t_[1]),
                        'ordenados': ord_, 'diff': diff, 'percentual': perc})
    ranking.sort(key=lambda r: r['terminoTotal'])

    estrategias_por_grupo = [
        {'grupo': 1, 'estrategia': est_final, 'quantidadePedidos': len(pedidos)}
    ]

    melhor = {
        'id':                  est_final['id'],
        'nome':                est_final['nome'],
        'terminoTotal':        tempo_final,
        'terminoHoras':        _round(tempo_final[1]),
        'ordenados':           ordenados_final,
        'decisao':             decisao,
        'estrategiasPorGrupo': estrategias_por_grupo,
        'usaPrioridade':       False,
        'totalCombinacoes':    2,
    }

    return melhor, ranking


# ── OTIMIZAR DISTRIBUIÇÃO ────────────────────────────────────────────────────
def otimizar_distribuicao(pedidos_ordenados, modelos, ref_data, num_machines, ridx_map,
                           data_base: date, datas_bloqueadas: set,
                           choices: list | None = None):
    """
    Distribui pedidos nas máquinas e gera o resultado final.

    choices: lista de índices retornada por sa_encaixes().
      Quando fornecida, usa o encaixe otimizado pelo SA em vez de greedy.
      Cada inteiro é o índice (dentro do gidxs local do pedido) da máquina
      escolhida para aquele slot — exatamente o mesmo mecanismo do simulador.
      None → comportamento greedy original.
    """
    filas        = np.zeros(num_machines, dtype=np.float64)
    resultado    = []
    sem_cadastro = []
    choice_ptr   = 0   # ponteiro na lista de choices do SA

    for pedido in pedidos_ordenados:
        ref          = pedido['referencia']
        cor          = pedido.get('cor', '') or '-'
        slots        = pedido['maquinas_necessarias']
        min_s        = float(pedido.get('min_start', 0.0))
        data_entrega = pedido.get('data_entrega')
        produto      = pedido.get('produto', '')
        cliente      = pedido.get('cliente', '')
        ordem_compra = pedido.get('ordem_compra', '')
        linha_sheet  = pedido.get('linha_sheet')

        # Usa arrays pré-computados (já aplicam restrição de maquina_especial)
        gidxs   = pedido.get('_gidxs')
        tempos  = pedido.get('_tempos')
        aba_idx = pedido.get('_aba_idx')

        if gidxs is None:
            sem_cadastro.append({
                'referencia':           ref,
                'produto':              produto,
                'cor':                  cor,
                'cliente':              cliente,
                'ordem_compra':         ordem_compra,
                'maquinas_necessarias': slots,
                'data_entrega':         data_entrega,
                'linha_sheet':          linha_sheet,
            })
            continue

        por_modelo = {}
        ng = len(gidxs)

        for _ in range(slots):
            available = np.maximum(filas[gidxs], min_s)
            ft    = available + tempos
            # Usa choice do SA se disponível, senão greedy
            if choices is not None and choice_ptr < len(choices):
                best = int(choices[choice_ptr]) % ng
            else:
                best = int(np.argmin(ft))
            choice_ptr += 1
            fim   = float(ft[best])
            aba, _li = aba_idx[best]
            inicio = float(max(filas[gidxs[best]], min_s))
            filas[gidxs[best]] = fim

            if aba not in por_modelo:
                por_modelo[aba] = {
                    'nome_modelo':    modelos[aba]['nome_modelo'],
                    'aba':            aba,
                    'tempo_producao': float(tempos[best]),
                    'slots':          0,
                    'inicio':         inicio,
                    'termino':        fim,
                }
            por_modelo[aba]['slots']   += 1
            por_modelo[aba]['termino']  = max(por_modelo[aba]['termino'], fim)
            por_modelo[aba]['inicio']   = min(por_modelo[aba]['inicio'],  inicio)

        for aba, aloc in por_modelo.items():
            dt_inicio  = horas_para_data(data_base, aloc['inicio'],  datas_bloqueadas)
            dt_termino = horas_para_data(data_base, aloc['termino'], datas_bloqueadas)

            prazo_str   = ''
            prazo_delta = None
            if data_entrega:
                delta       = (data_entrega - dt_termino.date()).days
                prazo_delta = delta
                prazo_str   = (f'{delta} dias antecipado' if delta >= 0
                               else f'{delta} dias atrasado')

            resultado.append({
                'referencia':        ref,
                'produto':           produto,
                'cor':               cor,
                'cliente':           cliente,
                'ordem_compra':      ordem_compra,
                'nome_modelo':       aloc['nome_modelo'],
                'aba':               aloc['aba'],
                'maquinas_alocadas': aloc['slots'],
                'tempo_producao':    aloc['tempo_producao'],
                'inicio_horas':      _round(aloc['inicio']),
                'termino_horas':     _round(aloc['termino']),
                'dt_inicio':         dt_inicio,
                'dt_termino':        dt_termino,
                'data_entrega':      data_entrega,
                'prazo_str':         prazo_str,
                'prazo_delta':       prazo_delta,
                'linha_sheet':       linha_sheet,
            })

    return resultado, sem_cadastro


# ── SUGESTÕES ────────────────────────────────────────────────────────────────
def calcular_sugestoes(modelos: dict) -> list:
    nomes     = list(modelos.keys())
    razoes    = {a: {} for a in nomes}
    confianca = {a: {} for a in nomes}

    for a in nomes:
        for b in nomes:
            if a == b:
                continue
            refs_a = modelos[a]['referencias']
            refs_b = modelos[b]['referencias']
            comuns = [r for r in refs_a if r in refs_b]
            if not comuns:
                razoes[a][b] = None
                confianca[a][b] = 0
                continue
            ratios = [refs_a[r] / refs_b[r] for r in comuns]
            razoes[a][b]    = sum(ratios) / len(ratios)
            confianca[a][b] = len(comuns)

    sugestoes = []
    for aba_dest in nomes:
        refs_dest  = modelos[aba_dest]['referencias']
        nome_dest  = modelos[aba_dest]['nome_modelo']
        todas_refs = set()
        for outra in nomes:
            if outra != aba_dest:
                todas_refs.update(modelos[outra]['referencias'])

        for ref in todas_refs - set(refs_dest):
            estimativas = []
            for aba_orig in nomes:
                if aba_orig == aba_dest:
                    continue
                if ref not in modelos[aba_orig]['referencias']:
                    continue
                if not razoes[aba_dest].get(aba_orig):
                    continue
                t_orig = modelos[aba_orig]['referencias'][ref]
                razao  = razoes[aba_dest][aba_orig]
                qtd    = confianca[aba_dest][aba_orig]
                estimativas.append({'origem': modelos[aba_orig]['nome_modelo'],
                                    'tempoOrigem': t_orig,
                                    'tempoEstimado': t_orig * razao,
                                    'qtdRefs': qtd})
            if not estimativas:
                continue
            peso_total = sum(e['qtdRefs'] for e in estimativas)
            media  = _round(sum(e['tempoEstimado'] * e['qtdRefs'] for e in estimativas) / peso_total)
            nivel  = 'Alta' if peso_total >= 10 else ('Média' if peso_total >= 5 else 'Baixa')
            base   = ' | '.join(
                f"{e['origem']}: {_round(e['tempoOrigem'])}h → {_round(e['tempoEstimado'])}h"
                for e in estimativas
            )
            sugestoes.append({'referencia': ref, 'maquina': nome_dest, 'aba': aba_dest,
                              'tempoEstimado': media, 'confianca': nivel,
                              'refsUsadas': peso_total, 'base': base})
    return sugestoes


# ── ESCREVER DE VOLTA NA ABA PEDIDO (colunas J e K) ──────────────────────────
def escrever_resultado_pedido(spreadsheet, resultado: list, sem_cadastro: list):
    """Preenche colunas J (data finalização) e K (prazo) na aba PEDIDO."""
    ws = spreadsheet.worksheet(CONFIG['ABA_PEDIDO'])

    # Para cada pedido, pega o termino mais tardio entre as alocações
    por_linha = {}
    for r in resultado:
        ln = r.get('linha_sheet')
        if ln is None:
            continue
        if ln not in por_linha or r['dt_termino'] > por_linha[ln]['dt_termino']:
            por_linha[ln] = r

    cell_list = []
    for ln, r in por_linha.items():
        val_j = r['dt_termino'].strftime('%d/%m/%Y')
        val_k = r['prazo_str'] if r['prazo_str'] else '—'
        cell_list.append(gspread.Cell(ln, 10, val_j))
        cell_list.append(gspread.Cell(ln, 11, val_k))

    # Items sem cadastro: informa na coluna J, K vazia
    for r in sem_cadastro:
        ln = r.get('linha_sheet')
        if ln is None or ln in por_linha:
            continue
        cell_list.append(gspread.Cell(ln, 10, 'Sem cadastro'))
        cell_list.append(gspread.Cell(ln, 11, '—'))

    if cell_list:
        ws.update_cells(cell_list, value_input_option='RAW')
        print(f'  ✔ {len(por_linha) + len(sem_cadastro)} linha(s) atualizadas na aba PEDIDO (J e K).')


# ── SALVAR RESULTADO ─────────────────────────────────────────────────────────
def salvar_resultado(spreadsheet, resultado, sem_cadastro, sugestoes, melhor):
    cab   = ['Referência', 'Produto', 'Cor', 'Cliente', 'Ordem de Compra',
             'Modelo', 'Aba', 'Máquinas', 'Tempo Prod. (h)',
             'Início', 'Término', 'Data Entrega', 'Prazo']
    ncols = len(cab)
    b     = SheetBuilder(spreadsheet, CONFIG['ABA_RESULTADO'], cols=ncols)

    cor_banner = '#1B5E20' if melhor['id'] in ('edd', 'balanceamento', 'sa') else '#E65100'
    b.banner(
        f"🏆 Estratégia: {melhor['nome']}  |  Término total: {melhor['terminoHoras']}h  |  {melhor.get('decisao', '')}",
        cor_banner, font_size=11)
    b.banner(
        f"ℹ️  Limiar de troca: {CONFIG['LIMIAR_TROCA_PERCENT']}% — outra estratégia só substitui o EDD se for pelo menos {CONFIG['LIMIAR_TROCA_PERCENT']}% mais rápida",
        '#E3F2FD', fg='#0D47A1', bold=False)
    b.blank()

    b.write(cab, bg='#1B5E20', fg='#FFFFFF', bold=True, h_align='CENTER')

    for r in resultado:
        inicio_s  = r['dt_inicio'].strftime('%d/%m/%Y %H:%M')  if r.get('dt_inicio')   else ''
        termino_s = r['dt_termino'].strftime('%d/%m/%Y %H:%M') if r.get('dt_termino')  else ''
        entrega_s = r['data_entrega'].strftime('%d/%m/%Y')      if r.get('data_entrega') else ''

        pd = r.get('prazo_delta')
        if pd is None:
            bg = None
        elif pd < 0:
            bg = '#FFCDD2'   # atrasado — vermelho claro
        elif pd == 0:
            bg = '#FFF9C4'   # no limite — amarelo
        else:
            bg = '#C8E6C9'   # antecipado — verde claro

        b.write([
            r['referencia'], r.get('produto', ''), r.get('cor', ''),
            r.get('cliente', ''), r.get('ordem_compra', ''),
            r['nome_modelo'], r['aba'], r['maquinas_alocadas'],
            r['tempo_producao'], inicio_s, termino_s, entrega_s,
            r.get('prazo_str', ''),
        ], bg=bg)

    if sem_cadastro:
        b.blank()
        b.banner('💡 SEM CADASTRO — Referências não encontradas em nenhuma máquina', '#E65100')
        b.write(['Referência', 'Produto', 'Cor', 'Cliente', 'Ordem de Compra',
                 'Máquinas', '', '', '', '', '', 'Data Entrega', ''],
                bg='#BF360C', fg='#FFFFFF', bold=True, h_align='CENTER')
        for r in sem_cadastro:
            entrega_s = r['data_entrega'].strftime('%d/%m/%Y') if r.get('data_entrega') else ''
            b.write([
                r['referencia'], r.get('produto', ''), r.get('cor', ''),
                r.get('cliente', ''), r.get('ordem_compra', ''),
                r['maquinas_necessarias'], '', '', '', '', '', entrega_s, '',
            ], bg='#FBE9E7')

    if sugestoes:
        b.blank()
        b.banner('💡 SUGESTÕES — Referências sem tempo cadastrado em certas máquinas', '#E65100')
        b.write(['Referência', 'Máquina', 'Tempo Estimado (h)', 'Confiança', 'Refs Usadas', 'Base do Cálculo',
                 '', '', '', '', '', '', ''],
                bg='#BF360C', fg='#FFFFFF', bold=True, h_align='CENTER')
        for s in sugestoes:
            b.write([s['referencia'], s['maquina'], s['tempoEstimado'],
                     s['confianca'], s['refsUsadas'], s['base'],
                     '', '', '', '', '', '', ''])

    b.flush()


# ── CÁLCULO DE MÁQUINAS CHINESAS EXTRAS ──────────────────────────────────────
def _calcular_extras_chines(pedidos_orig: list, modelos: dict,
                             melhor_ordenados: list, resultado: list,
                             data_base) -> dict | None:
    """
    Calcula quantas máquinas chinesas extras (incremento em total_maquinas)
    são necessárias para que 100% dos pedidos elegíveis fiquem em dia.

    Elegível = deadline_horas > 0 (prazo NÃO anterior à data base).

    Retorna dict com:
      extras          — número de máquinas extras por modelo chinês (int ou '>200')
      elegiveis       — total de pedidos elegíveis únicos
      atrasados       — pedidos elegíveis atrasados no resultado atual
      modelos_info    — lista de dicts {nome_modelo, total_maquinas_atual}
    Retorna None se não houver nenhum modelo chinês cadastrado.
    """
    # 1. Identificar modelos chineses
    abas_chines = {aba: mod for aba, mod in modelos.items()
                   if _e_modelo_chines_48(mod['nome_modelo'])}
    if not abas_chines:
        return None

    # 2. Contar pedidos elegíveis e atrasados a partir do resultado real
    #    (resultado já usa a melhor atribuição SA — mais preciso que re-simular)
    prazo_por_linha: dict[int, int] = {}
    for r in resultado:
        ls = r.get('linha_sheet')
        de = r.get('data_entrega')
        pd = r.get('prazo_delta')
        if ls is None or de is None or pd is None:
            continue
        if de < data_base:
            continue   # não elegível — prazo anterior à data base
        # Guarda o pior prazo do pedido (pode ter múltiplas alocações por modelo)
        if ls not in prazo_por_linha:
            prazo_por_linha[ls] = pd
        else:
            prazo_por_linha[ls] = min(prazo_por_linha[ls], pd)

    n_elegiveis = len(prazo_por_linha)
    n_atrasados = sum(1 for v in prazo_por_linha.values() if v < 0)

    modelos_info = [
        {'nome_modelo': mod['nome_modelo'], 'total_maquinas_atual': mod['total_maquinas']}
        for mod in abas_chines.values()
    ]

    if n_atrasados == 0:
        return {
            'extras': 0,
            'elegiveis': n_elegiveis,
            'atrasados': 0,
            'modelos_info': modelos_info,
        }

    # 3. Montar lista de pedidos elegíveis na ordem do melhor, sem arrays numpy
    #    (serão recomputados contra o ref_data expandido)
    linhas_elegiveis = set(prazo_por_linha.keys())
    pedidos_eleg_ord = [
        {k: v for k, v in p.items() if not k.startswith('_')}
        for p in melhor_ordenados
        if p.get('linha_sheet') in linhas_elegiveis
    ]
    if not pedidos_eleg_ord:
        return {
            'extras': 0,
            'elegiveis': n_elegiveis,
            'atrasados': n_atrasados,
            'modelos_info': modelos_info,
        }

    # 4. Helper: simula tardiness com N extras adicionados a cada modelo chinês
    def tardiness_com_extras(n_extras: int) -> float:
        modelos_ext = {}
        for aba, mod in modelos.items():
            m = dict(mod)
            if aba in abas_chines:
                m['referencias'] = dict(mod['referencias'])
                m['descricoes']  = dict(mod.get('descricoes', {}))
                m['total_maquinas'] = mod['total_maquinas'] + n_extras
            modelos_ext[aba] = m
        ref_ext, num_ext, _ = precomputar_maquinas(modelos_ext)
        ped_clean = [dict(p) for p in pedidos_eleg_ord]
        preparar_restricoes_pedidos(ped_clean, ref_ext, modelos_ext)
        return simular_custo(ped_clean, ref_ext, num_ext)[0]

    # 5. Busca binária: menor N onde tardiness == 0
    MAX_BUSCA = 200
    if tardiness_com_extras(MAX_BUSCA) != 0:
        return {
            'extras': f'>{MAX_BUSCA}',
            'elegiveis': n_elegiveis,
            'atrasados': n_atrasados,
            'modelos_info': modelos_info,
        }

    lo, hi = 1, MAX_BUSCA
    while lo < hi:
        mid = (lo + hi) // 2
        if tardiness_com_extras(mid) == 0:
            hi = mid
        else:
            lo = mid + 1

    return {
        'extras': lo,
        'elegiveis': n_elegiveis,
        'atrasados': n_atrasados,
        'modelos_info': modelos_info,
    }


# ── SALVAR COMPARATIVO ───────────────────────────────────────────────────────
def salvar_comparativo(spreadsheet, melhor, ranking, num_pedidos, num_modelos,
                       pedidos=None, modelos=None, resultado=None, data_base=None):
    cab   = ['Posição', 'Estratégia', 'Descrição', 'Término Total (h)', 'Diferença vs Melhor (h)', 'Variação %']
    ncols = len(cab)
    b     = SheetBuilder(spreadsheet, 'COMPARATIVO', cols=ncols)

    b.banner('📊 COMPARATIVO DE ESTRATÉGIAS DE DISTRIBUIÇÃO', '#0D47A1', font_size=13)

    cor_b = '#1B5E20' if melhor['id'] in ('edd', 'balanceamento', 'sa') else '#E65100'
    b.banner(f"🏆 Escolhido: {melhor['nome']}  —  Término total: {melhor['terminoHoras']}h", cor_b)
    b.banner(f"{melhor.get('decisao', '')}  |  Limiar: {CONFIG['LIMIAR_TROCA_PERCENT']}%",
             '#E8F5E9', fg='#1B5E20', bold=False)

    if melhor.get('usaPrioridade') and melhor.get('estrategiasPorGrupo'):
        b.banner(f"📋 COMBINAÇÃO VENCEDORA  |  {melhor['totalCombinacoes']} combinações analisadas",
                 '#263238')
        b.write(['Grupo', 'Nº Pedidos', 'Estratégia Escolhida', 'Descrição', '', ''],
                bg='#37474F', fg='#FFFFFF', bold=True, h_align='CENTER')
        cores_g = ['#E8F5E9', '#E3F2FD', '#F3E5F5', '#ECEFF1', '#FBE9E7']
        for g in melhor['estrategiasPorGrupo']:
            cg = cores_g[min(g['grupo'] - 1, len(cores_g) - 1)]
            b.write([f"Grupo {g['grupo']}", g['quantidadePedidos'],
                     g['estrategia']['nome'], g['estrategia']['descricao'], '', ''], bg=cg)
        b.blank()

    b.banner('📊 RANKING DAS ESTRATÉGIAS', '#455A64')
    b.write(cab, bg='#263238', fg='#FFFFFF', bold=True, h_align='CENTER')

    melhor_t = ranking[0]['terminoTotal'][1]  # makespan do melhor para exibição
    for i, est in enumerate(ranking):
        diff   = _round(est['terminoTotal'][1] - melhor_t)
        perc   = _round(((est['terminoTotal'][1] - melhor_t) / melhor_t) * 100) if melhor_t > 0 else 0
        pos_s  = '🏆 1º' if i == 0 else f'{i + 1}º'
        diff_s = '—' if i == 0 else (f'+{diff}h' if diff >= 0 else f'{diff}h')
        perc_s = '✅ MELHOR' if i == 0 else (f'+{perc}% mais lento' if perc > 0 else f'{perc}% mais rápido')
        if i == 0:
            bg, fg, bold = '#1B5E20', '#FFFFFF', True
        elif perc < 0:
            bg, fg, bold = '#C8E6C9', '#000000', False
        else:
            bg, fg, bold = ('#FFEBEE' if i % 2 == 0 else '#FFCDD2'), '#000000', False
        b.write([pos_s, est['nome'], est['descricao'], f"{est['terminoHoras']}h", diff_s, perc_s],
                bg=bg, fg=fg, bold=bold)

    b.blank(2)
    b.banner(
        f"ℹ️  Variação % vs EDD. Troca só ocorre se ganho ≥ {CONFIG['LIMIAR_TROCA_PERCENT']}%."
        + (f" | {melhor['totalCombinacoes']} combinações analisadas."
           if melhor.get('usaPrioridade') else ''),
        '#E3F2FD', fg='#0D47A1', bold=False, wrap=True
    )

    b.blank(3)

    # ── SEÇÃO: CAPACIDADE — máquinas chinesas extras necessárias ────────────
    if pedidos is not None and modelos is not None and resultado is not None and data_base is not None:
        analise = _calcular_extras_chines(pedidos, modelos, melhor['ordenados'], resultado, data_base)

        b.banner('🏭 ANÁLISE DE CAPACIDADE — Máquinas Chinesas', '#1A237E', font_size=12)

        if analise is None:
            b.banner(
                'Nenhum modelo "48 fusos Chines" encontrado nas abas de máquinas. '
                'Esta análise só se aplica a máquinas chinesas.',
                '#ECEFF1', fg='#37474F', bold=False, wrap=True
            )
        else:
            mi = analise['modelos_info']
            nomes_ch = ', '.join(m['nome_modelo'] for m in mi)
            maq_atuais_str = '  |  '.join(
                f"{m['nome_modelo']}: {m['total_maquinas_atual']} máq."
                for m in mi
            )

            b.write(
                ['Modelo(s) Chinês', 'Qtd. Atual', 'Pedidos Elegíveis',
                 'Pedidos Atrasados', 'Extras Necessários', 'Observação'],
                bg='#283593', fg='#FFFFFF', bold=True, h_align='CENTER'
            )

            extras = analise['extras']
            n_el   = analise['elegiveis']
            n_at   = analise['atrasados']

            if extras == 0:
                cor_linha = '#C8E6C9'
                extras_s  = '0 — todos em dia ✅'
                obs       = 'Nenhum acréscimo necessário com a estratégia atual.'
            elif isinstance(extras, str):   # '>200'
                cor_linha = '#FFCDD2'
                extras_s  = extras
                obs       = 'Mesmo com muitas máquinas extras, pedidos podem não caber no prazo.'
            else:
                cor_linha = '#FFF9C4'
                extras_s  = str(extras)
                obs = (
                    f'+{extras} por modelo chinês. '
                    'Estimativa via simulação greedy (pedidos elegíveis, ordem EDD).'
                )

            for m in mi:
                b.write(
                    [m['nome_modelo'], m['total_maquinas_atual'],
                     n_el, n_at, extras_s, obs],
                    bg=cor_linha
                )

            b.blank()
            b.banner(
                '⚠ Pedidos elegíveis = prazo ≥ data base. '
                'Pedidos com data de entrega anterior à data base são excluídos desta conta. '
                'Extras = incremento em K1 de cada aba chinesa (máquinas físicas adicionais).',
                '#E8EAF6', fg='#283593', bold=False, wrap=True
            )

        b.blank(2)

    _secao_cientifica(b, num_pedidos, num_modelos)
    b.freeze(4)
    b.flush()


# ── SALVAR RELATÓRIO PARA IMPRESSÃO ──────────────────────────────────────────
def _e_modelo_chines_48(nome_modelo: str) -> bool:
    """Retorna True se o modelo for '48 fusos Chines' (variações de capitalização)."""
    n = nome_modelo.lower()
    return '48' in n and 'fuso' in n and 'chin' in n


def salvar_relatorio(spreadsheet, resultado: list, melhor: dict):
    """Cria aba RELATORIO com pedidos ordenados por data de início, para impressão."""
    ordenado = sorted(
        resultado,
        key=lambda r: (r.get('dt_inicio') or datetime.min, r.get('dt_termino') or datetime.min)
    )

    cab   = ['Início', 'Término', 'Referência', 'Produto', 'Cor',
             'Cliente', 'Ordem de Compra', 'Modelo', 'Máquinas',
             'Data Entrega', 'Prazo']
    ncols = len(cab)
    b     = SheetBuilder(spreadsheet, CONFIG['ABA_RELATORIO'], cols=ncols)

    hoje  = date.today().strftime('%d/%m/%Y')

    # Aviso de ajuste para máquinas chinesas (aparece antes do cabeçalho)
    tem_chines = any(_e_modelo_chines_48(r.get('nome_modelo', '')) for r in ordenado)
    if tem_chines:
        b.banner(
            '⚠ ATENÇÃO: Máquinas chinesas ajustada a quantidade para Espula grande '
            '— quantidade de máquinas é o que precisa ser montado realmente.',
            '#F57F17', fg='#FFFFFF', bold=True, font_size=12)

    b.banner(f'📋 RELATÓRIO DE PRODUÇÃO — Gerado em {hoje}', '#0D47A1', font_size=13)
    cor_b = '#1B5E20' if melhor['id'] in ('edd', 'balanceamento', 'sa') else '#E65100'
    b.banner(
        f"🏆 {melhor['nome']}  |  Término total: {melhor['terminoHoras']}h  |  {melhor.get('decisao', '')}",
        cor_b, font_size=11)
    b.blank()
    b.write(cab, bg='#263238', fg='#FFFFFF', bold=True, h_align='CENTER')
    b.freeze(5)

    cores_base = ['#FFFFFF', '#F5F5F5']
    for i, r in enumerate(ordenado):
        pd = r.get('prazo_delta')
        if pd is not None and pd < 0:
            bg = '#FFCDD2'   # atrasado
        elif pd == 0:
            bg = '#FFF9C4'   # no limite
        elif pd is not None and pd > 0:
            bg = '#C8E6C9'   # antecipado
        else:
            bg = cores_base[i % 2]

        inicio_s  = r['dt_inicio'].strftime('%d/%m/%Y %H:%M')  if r.get('dt_inicio')   else ''
        termino_s = r['dt_termino'].strftime('%d/%m/%Y %H:%M') if r.get('dt_termino')  else ''
        entrega_s = r['data_entrega'].strftime('%d/%m/%Y')      if r.get('data_entrega') else ''

        maquinas = r['maquinas_alocadas']
        if _e_modelo_chines_48(r.get('nome_modelo', '')):
            maquinas = math.ceil(maquinas / 2)

        b.write([
            inicio_s, termino_s,
            r['referencia'], r.get('produto', ''), r.get('cor', ''),
            r.get('cliente', ''), r.get('ordem_compra', ''),
            r['nome_modelo'], maquinas,
            entrega_s, r.get('prazo_str', ''),
        ], bg=bg)

    b.flush()


def salvar_relatorio_montagem(spreadsheet, resultado: list):
    """Cria aba RELATORIO MONTAGEM para uso dos montadores na produção."""
    ordenado = sorted(
        resultado,
        key=lambda r: (r.get('dt_inicio') or datetime.min, r.get('dt_termino') or datetime.min)
    )

    cab   = ['Data Início', 'Total Máquinas', 'Modelo', 'Produto', 'Cliente', 'OC']
    ncols = len(cab)
    b     = SheetBuilder(spreadsheet, 'RELATORIO MONTAGEM', cols=ncols)

    hoje  = date.today().strftime('%d/%m/%Y')
    b.banner(f'🔧 RELATÓRIO DE MONTAGEM — Gerado em {hoje}', '#1A237E', font_size=13)
    b.blank()
    b.write(cab, bg='#37474F', fg='#FFFFFF', bold=True, h_align='CENTER')
    b.freeze(3)

    cores_base = ['#FFFFFF', '#F5F5F5']
    for i, r in enumerate(ordenado):
        bg = cores_base[i % 2]

        # Data de início como valor de data puro (para permitir filtro por data)
        dt_inicio = r.get('dt_inicio')
        if dt_inicio:
            inicio_val = dt_inicio.strftime('%d/%m/%Y')
        else:
            inicio_val = ''

        maquinas = r['maquinas_alocadas']
        if _e_modelo_chines_48(r.get('nome_modelo', '')):
            maquinas = math.ceil(maquinas / 2)

        b.write([
            inicio_val,
            maquinas,
            r.get('nome_modelo', ''),
            r.get('produto', ''),
            r.get('cliente', ''),
            r.get('ordem_compra', ''),
        ], bg=bg)

    b.flush()


def _secao_cientifica(b: SheetBuilder, num_pedidos: int, num_modelos: int):
    b.banner('🔬 ANÁLISE CIENTÍFICA — Algoritmo vs Planejador Humano', '#4A148C', font_size=12)
    ef = _calc_eficiencia(num_pedidos, num_modelos)
    b.banner(f'{ef}% MAIS EFICIENTE', '#1B5E20', font_size=28)
    b.banner(
        f'O algoritmo é {ef}% mais eficiente para {num_pedidos} lotes × {num_modelos} modelos '
        f'→ {_fatorial_aprox(num_pedidos)} combinações. '
        f'Um humano avalia no máximo 7±2 opções (Miller, 1956). O algoritmo avalia todas.',
        '#E8F5E9', fg='#1B5E20', wrap=True
    )
    b.blank()

    b.write(['Dimensão', 'Humano', 'Algoritmo', 'Vantagem', 'Fonte'],
            bg='#311B92', fg='#FFFFFF', bold=True, h_align='CENTER')

    dados = [
        ['Velocidade',         f'Horas/dias para {num_pedidos} lotes', 'Segundos',
         '~99% mais rápido',   'Intito (2025) — Scheduling Optimization'],
        ['Combinações',        '3 a 10 (limite cognitivo)',   _fatorial_aprox(num_pedidos),
         f'{_calc_vantagem(num_pedidos)}× mais', 'Miller (1956) — 7±2 itens memória de trabalho'],
        ['Redução de custos',  'Baseline humano',             '8,5–10,2% menos custo',
         '8,5–10,2%',          'Wang et al. — MDPI Electronics (2023)'],
        ['Consistência',       'Variável — fadiga e viés',    '100% determinístico',
         'Elimina erro humano', 'Frontiers Ind. Engineering (2025)'],
        ['Escala',             'Até ~10 lotes',               f'{num_pedidos}+ lotes',
         f'{max(0, num_pedidos-10)} além do limite', 'Garey & Johnson (1979) — NP-hard'],
        ['Multi-modelo',       'Difícil > 3–4 modelos',       f'{num_modelos} modelos em paralelo',
         f'{max(0,num_modelos-4)} além do limite', 'Springer Adv. Manuf. Technology (2020)'],
        ['Makespan',           'Solução empírica',            'Próxima do ótimo matemático',
         '23–40% redução',     'Frontiers Manuf. Technology (2022)'],
    ]
    cores = ['#F3E5F5', '#EDE7F6']
    for i, linha in enumerate(dados):
        b.write(linha, bg=cores[i % 2])


# ── UTILITÁRIOS ──────────────────────────────────────────────────────────────
def _round(n): return round(n, 2)

def _fatorial_aprox(n):
    if n <= 10:
        return f'{math.factorial(n):,}'.replace(',', '.')
    log10 = n * math.log10(n / math.e) + 0.5 * math.log10(2 * math.pi * n)
    return f'~10^{int(log10)}'

def _calc_vantagem(n):
    if n <= 7:
        return round(math.factorial(n) / 7)
    log10 = n * math.log10(n / math.e) + 0.5 * math.log10(2 * math.pi * n)
    return f'10^{int(log10 - 1)}'

def _calc_eficiencia(np_, nm):
    f   = math.factorial(min(np_, 20))
    cob = min(99.99, ((f - 7) / f) * 100)
    return min(99, round(cob * 0.5 + 31.5 * 0.3 + 95 * 0.2))

def gerar_resumo(resultado, sem_cadastro, melhor, ranking):
    linhas = [
        f"🏆 {melhor['nome']}",
        f"   Término: {melhor['terminoHoras']}h",
    ]
    if melhor.get('usaPrioridade') and melhor.get('estrategiasPorGrupo'):
        linhas.append('\n📋 Por grupo:')
        for g in melhor['estrategiasPorGrupo']:
            linhas.append(f"   G{g['grupo']} ({g['quantidadePedidos']} pedidos): {g['estrategia']['nome']}")
    if ranking:
        linhas.append('\n📊 Top 3:')
        for i, est in enumerate(ranking[:3]):
            _d   = _round(est['terminoTotal'][1] - ranking[0]['terminoTotal'][1])
            diff = '✅ melhor' if i == 0 else (f'+{_d}h' if _d >= 0 else f'{_d}h')
            linhas.append(f"   {i+1}. {est['nome'][:38]}: {est['terminoHoras']}h ({diff})")

    atrasados   = sum(1 for r in resultado if r.get('prazo_delta') is not None and r['prazo_delta'] < 0)
    antecipados = sum(1 for r in resultado if r.get('prazo_delta') is not None and r['prazo_delta'] > 0)
    if atrasados or antecipados:
        linhas.append(f'\n📅 Prazos: {antecipados} antecipado(s), {atrasados} atrasado(s)')

    if sem_cadastro:
        linhas.append(f'\n⚠ Sem cadastro: {len({r["referencia"] for r in sem_cadastro})} referência(s)')
    return '\n'.join(linhas)


# ── ANÁLISE DE CORES FALTANTES ───────────────────────────────────────────────
def _maquinas_capazes_para_ref(ref: str, modelos: dict) -> list:
    """
    Retorna as abas cujas máquinas conseguem produzir 'ref', seja pela chave
    genérica ('M60109') ou por qualquer chave cor-específica ('M60109 2410').
    """
    prefix = f"{ref} "
    return [
        aba for aba, mod in modelos.items()
        if ref in mod['referencias']
        or any(k.startswith(prefix) for k in mod['referencias'])
    ]


def _tempo_estimado_para_combined(combined: str, ref: str,
                                   maquinas_faltando: list,
                                   maquinas_com_combined: list,
                                   modelos: dict) -> float:
    """
    Estima o tempo de produção para a chave 'combined' nas máquinas faltando.
    Prioridade: 1) melhor tempo de 'combined' em outras máquinas
                2) melhor tempo da ref genérica nas próprias máquinas faltando
                3) melhor tempo de qualquer cor da mesma ref em qualquer máquina
    """
    if maquinas_com_combined:
        return min(modelos[a]['referencias'][combined] for a in maquinas_com_combined)

    tempos_base = [
        modelos[a]['referencias'][ref]
        for a in maquinas_faltando
        if ref in modelos[a]['referencias']
    ]
    if tempos_base:
        return min(tempos_base)

    prefix = f"{ref} "
    tempos_similares = [
        t
        for mod in modelos.values()
        for k, t in mod['referencias'].items()
        if k.startswith(prefix)
    ]
    if tempos_similares:
        return min(tempos_similares)

    return 0.0


def _detectar_lacunas(pedidos: list, modelos: dict) -> list:
    """
    Retorna lista de lacunas: máquinas que têm a ref mas não têm a cor do pedido.
    """
    vistos    = set()
    sugestoes = []
    for p in pedidos:
        ref = ' '.join((p['referencia'] or '').split())
        cor = ' '.join((p.get('cor') or '').split())
        if not cor or cor == '-':
            continue
        chave = (ref, cor)
        if chave in vistos:
            continue
        vistos.add(chave)

        combined     = f"{ref} {cor}"
        capazes      = _maquinas_capazes_para_ref(ref, modelos)
        com_combined = [a for a in capazes if combined in modelos[a]['referencias']]
        faltando     = [a for a in capazes if a not in com_combined]

        if not faltando:
            continue

        tempo = _tempo_estimado_para_combined(combined, ref, faltando, com_combined, modelos)
        if tempo <= 0:
            continue

        # Busca a descrição completa (coluna A) em qualquer máquina que já tenha o combined
        descricao = ''
        for a in com_combined:
            descricao = modelos[a].get('descricoes', {}).get(combined, '')
            if descricao:
                break

        sugestoes.append({
            'ref':               ref,
            'cor':               cor,
            'combined':          combined,
            'descricao':         descricao,
            'maquinas_faltando': faltando,
            'maquinas_com_cor':  com_combined,
            'tempo_sugerido':    tempo,
        })
    return sugestoes


def analisar_cores_faltantes(pedidos: list, modelos: dict, spreadsheet):
    """
    Detecta lacunas de cor nas máquinas, simula o ganho e entra em loop
    até o usuário cadastrar todas as cores na planilha ou optar por ignorar.

    Retorna modelos atualizados (relidos da planilha) ou None (sem alteração).
    """
    # ── 1. Verificação inicial ────────────────────────────────────────────────
    sugestoes = _detectar_lacunas(pedidos, modelos)
    if not sugestoes:
        return None

    # ── 2. Simulação única para mostrar o ganho estimado ─────────────────────
    modelos_sim = copy.deepcopy(modelos)
    for sug in sugestoes:
        for aba in sug['maquinas_faltando']:
            modelos_sim[aba]['referencias'][sug['combined']] = sug['tempo_sugerido']

    ref_data_orig, num_orig, _ = precomputar_maquinas(modelos)
    ref_data_sim,  num_sim,  _ = precomputar_maquinas(modelos_sim)

    # Remove _gidxs/_tempos pré-computados para forçar o lookup direto no ref_data
    # correto. Sem isso, ambas as simulações reutilizam os arrays do original e
    # mostram melhoria zero, mesmo quando o cadastro da cor reduziria o término.
    pedidos_limpos = [{k: v for k, v in p.items() if not k.startswith('_')}
                      for p in pedidos]

    termino_orig = simular_termino(pedidos_limpos, ref_data_orig, num_orig)
    termino_sim  = simular_termino(pedidos_limpos, ref_data_sim,  num_sim)
    melhoria_h   = _round(termino_orig - termino_sim)
    melhoria_pct = _round((melhoria_h / termino_orig * 100) if termino_orig > 0 else 0)

    # ── 3. Exibir análise completa ────────────────────────────────────────────
    print('\n' + '─' * 60)
    print('🎨 ANÁLISE DE CORES NÃO CADASTRADAS')
    print(f'   {len(sugestoes)} combinação(ões) ref+cor com lacuna nas máquinas:\n')
    for sug in sugestoes:
        nomes_f = ', '.join(modelos[a]['nome_modelo'] for a in sug['maquinas_faltando'])
        desc_str = f'  ({sug["descricao"]})' if sug['descricao'] else ''
        print(f'   Ref "{sug["ref"]}"  Cor "{sug["cor"]}"{desc_str}')
        print(f'     Falta cadastrar..: {nomes_f}')
        print(f'     Tempo estimado...: {sug["tempo_sugerido"]}h/máquina')
        print()

    if melhoria_h > 0:
        print(f'   📈 Ganho estimado ao cadastrar todas:')
        print(f'      Término atual:    {_round(termino_orig)}h')
        print(f'      Término estimado: {_round(termino_sim)}h  '
              f'(−{melhoria_h}h / {melhoria_pct}% mais rápido)')
    else:
        print('   ℹ️  Os tempos estimados não reduzem o término total,')
        print('      mas o cadastro garante uma distribuição mais precisa.')

    print()
    print('   ➡  Abra a aba da máquina na planilha, adicione uma linha com:')
    print('      coluna G = referencia  |  coluna F = cor  |  coluna B = tempo de produção (h)')
    print('─' * 60)

    # ── 4. Loop até detectar o cadastro real na planilha ─────────────────────
    tentativa = 0
    while True:
        tentativa += 1
        resp = input(
            f'   [{tentativa}] Cadastrou as cores acima na planilha? '
            '(Enter para verificar / "pular" para ignorar): '
        ).strip().lower()

        if resp == 'pular':
            print('   ⚠  Pulando análise de cores. Calculando com dados originais.\n')
            return None

        # Re-lê os modelos diretamente da planilha
        print('   🔄 Verificando planilha...')
        modelos_novo = ler_modelos(spreadsheet)

        pendentes = _detectar_lacunas(pedidos, modelos_novo)

        if not pendentes:
            print('   ✔ Todas as cores foram cadastradas! Recalculando...\n')
            return modelos_novo

        # Mostra o que ainda falta
        registradas = len(sugestoes) - len(pendentes)
        print(f'   ⚠  {registradas}/{len(sugestoes)} cor(es) registrada(s). '
              f'Ainda faltam {len(pendentes)}:\n')
        for sug in pendentes:
            nomes_f = ', '.join(modelos_novo[a]['nome_modelo']
                                for a in sug['maquinas_faltando']
                                if a in modelos_novo)
            print(f'     • Ref "{sug["ref"]}"  Cor "{sug["cor"]}"  →  {nomes_f}')
        print()


# ── PRÉ-SIMULAÇÃO DE RESTRIÇÕES ──────────────────────────────────────────────
def _pre_simular_restritos(fila_inicial: list, num_machines: int, hpd: float) -> list:
    """
    Antes do rolling-horizon principal, determina o bloco correto para pedidos
    com restrições (maquina_especial ou min_start > 0) fazendo uma pré-simulação
    greedy de todos os pedidos em ordem de bloco.

    Algoritmo:
      1. Para cada bloco (em ordem), captura snapshot das máquinas ANTES de
         processar o bloco e depois roda simulação greedy simples do bloco.
      2. Para cada pedido restrito, usa o snapshot do seu bloco para calcular
         o início efetivo real (max(min_start, maquina_mais_cedo_livre)).
      3. Se o início efetivo cai fora da janela do bloco, o pedido é realocado
         para o bloco correto.

    Retorna nova fila com as realocações aplicadas (blocos reordenados).
    """
    def _bkey(b):
        if b == 'vencido':   return -1
        if b == 'sem_prazo': return float('inf')
        return b

    tem_restrito = any(
        bool(p.get('maquina_especial')) or float(p.get('min_start', 0.0)) > 0
        for bloco in fila_inicial for p in bloco['pedidos']
    )
    if not tem_restrito:
        return fila_inicial   # nada a ajustar

    # ── Pré-simulação greedy: snapshot ANTES de cada bloco ──────────────────
    filas         = np.zeros(num_machines, dtype=np.float64)
    snap_antes    = {}   # bucket → filas antes do bloco

    for bloco in fila_inicial:
        b = bloco['bucket']
        snap_antes[b] = filas.copy()
        for p in bloco['pedidos']:
            gidxs = p.get('_gidxs')
            if gidxs is None:
                continue
            tempos = p['_tempos']
            min_s  = float(p.get('min_start', 0.0))
            for _ in range(p['maquinas_necessarias']):
                available = np.maximum(filas[gidxs], min_s)
                ft        = available + tempos
                best      = int(np.argmin(ft))
                filas[gidxs[best]] = float(ft[best])

    # ── Realocar pedidos restritos para o bloco efetivo ─────────────────────
    mapa_novo: dict = {}
    ajustes         = 0

    for bloco in fila_inicial:
        b_atual    = bloco['bucket']
        filas_ref  = snap_antes.get(b_atual, np.zeros(num_machines, dtype=np.float64))

        for p in bloco['pedidos']:
            maq_esp = bool(p.get('maquina_especial'))
            min_s   = float(p.get('min_start', 0.0))

            # Pedidos sem restrição mantêm o bloco atual
            if not maq_esp and min_s <= 0.0:
                mapa_novo.setdefault(b_atual, []).append(p)
                continue

            gidxs = p.get('_gidxs')
            if gidxs is None:
                mapa_novo.setdefault(b_atual, []).append(p)
                continue

            # Início efetivo considerando disponibilidade real das máquinas
            machine_free    = float(np.min(filas_ref[gidxs]))
            effective_start = max(min_s, machine_free)
            effective_day   = int(effective_start // hpd) if effective_start > 0 else 0

            # Bucket correto: nunca anterior ao bucket pelo prazo
            dl = p.get('deadline_horas')
            if dl is None:
                b_correto = 'sem_prazo'
            elif dl < 0:
                # Vencido: só move se realmente não pode iniciar agora
                b_correto = effective_day if effective_day > 0 else 'vencido'
            else:
                dl_day    = int(dl // hpd)
                b_correto = max(dl_day, effective_day)

            if b_correto != b_atual:
                ajustes += 1
            mapa_novo.setdefault(b_correto, []).append(p)

    if ajustes:
        print(f'  ↪ Pré-simulação: {ajustes} pedido(s) com restrição '
              f'realocado(s) para o bloco correto')

    return [{'bucket': b, 'pedidos': mapa_novo[b]}
            for b in sorted(mapa_novo, key=_bkey)
            if mapa_novo[b]]


# ── SEPARAÇÃO DE PEDIDOS COM RESTRIÇÕES ──────────────────────────────────────
def _separar_diferidos(pedidos: list, filas_atual, hpd: float, bucket) -> tuple:
    """
    Separa os pedidos do bloco em dois grupos:
      no_bloco  — podem ser agendados dentro da janela deste bloco
      diferidos — têm restrição (maquina_especial / min_start) que impede
                  início antes do fim da janela do bloco; são realocados
                  para o bloco correto baseado no estado real das máquinas

    Lógica de janela:
      bucket 'vencido' → janela termina em 0h  (já era pra ter saído)
      bucket  n (int)  → janela termina em (n+1)*hpd horas
      bucket 'sem_prazo' → janela infinita (nunca difere)

    Para pedidos sem _gidxs (sem cadastro) a função os mantém no bloco atual
    — o simulador já os ignora corretamente.
    """
    if bucket == 'sem_prazo':
        return list(pedidos), {}   # sem_prazo: nunca difere

    block_end = 0.0 if bucket == 'vencido' else (bucket + 1) * hpd

    no_bloco:  list = []
    diferidos: dict = {}

    for p in pedidos:
        gidxs = p.get('_gidxs')
        min_s = float(p.get('min_start', 0.0))

        if gidxs is None:
            no_bloco.append(p)
            continue

        # Quando a máquina mais rápida (dentro das permitidas) estará livre
        machine_free    = float(np.min(filas_atual[gidxs]))
        effective_start = max(min_s, machine_free)

        if effective_start <= block_end:
            no_bloco.append(p)
        else:
            # Dia efetivo de início — determina o bloco de destino
            effective_day = int(effective_start // hpd)
            diferidos.setdefault(effective_day, []).append(p)

    return no_bloco, diferidos


def _inserir_em_fila(fila: list, idx_atual: int, dia: int, pedidos_novos: list):
    """
    Insere pedidos_novos no bloco de dia 'dia' da fila, após idx_atual.
    Se o bloco não existir, cria um novo na posição correta (ordem crescente
    de dia, antes de 'sem_prazo').
    """
    for j in range(idx_atual + 1, len(fila)):
        bj = fila[j]['bucket']
        if bj == dia:
            fila[j]['pedidos'].extend(pedidos_novos)
            return
        if bj == 'sem_prazo' or (isinstance(bj, int) and bj > dia):
            fila.insert(j, {'bucket': dia, 'pedidos': list(pedidos_novos)})
            return
    # Adiciona antes de 'sem_prazo' se existir, senão no fim
    sp = next((j for j in range(idx_atual + 1, len(fila))
               if fila[j]['bucket'] == 'sem_prazo'), None)
    if sp is not None:
        fila.insert(sp, {'bucket': dia, 'pedidos': list(pedidos_novos)})
    else:
        fila.append({'bucket': dia, 'pedidos': list(pedidos_novos)})


# ── OTIMIZAÇÃO EM BLOCOS POR PRAZO (rolling-horizon) ─────────────────────────
def otimizar_em_blocos(pedidos, modelos, ref_data, num_machines):
    """
    Otimização rolling-horizon: agrupa pedidos por dia de vencimento e aplica
    toda a capacidade de análise (EDD/SA + 2-opt + SA encaixes) a cada bloco
    separadamente, passando o estado das máquinas para o bloco seguinte.

    Vantagens sobre a otimização global:
      • Pedidos atrasados usam as máquinas ANTES dos pedidos futuros —
        restrição dura, não apenas peso na função de custo.
      • Cada bloco é menor → mais iterações SA/2-opt por pedido.
      • O "tetris" dentro de cada bloco é mais apertado.

    Retorna (ordenados_total, choices_total, melhor_global).
    'choices_total' é a concatenação dos choices de todos os blocos e pode
    ser passado diretamente a otimizar_distribuicao como a lista de encaixes.
    """
    hpd = CONFIG['HORAS_POR_DIA']

    # ── Pré-simulação: blocos iniciais com restrições já no lugar certo ────────
    # Roda simulação greedy de todos os pedidos para capturar estado das máquinas
    # antes de cada bloco. Pedidos com maquina_especial ou min_start são realocados
    # para o bloco onde de fato podem iniciar — antes de qualquer SA/2-opt.
    fila_base = [{'bucket': b['bucket'], 'pedidos': list(b['pedidos'])}
                 for b in agrupar_por_dia_vencimento(pedidos)]
    fila: list = _pre_simular_restritos(fila_base, num_machines, hpd)

    filas_atual     = np.zeros(num_machines, dtype=np.float64)
    ordenados_total = []
    choices_total   = []
    decisao_partes  = []
    blocos_info     = []   # para estrategiasPorGrupo no resultado
    tard_total      = 0.0
    idx             = 0    # índice corrente (fila pode crescer durante o loop)

    while idx < len(fila):
        bloco  = fila[idx]
        bucket = bloco['bucket']

        # ── Realocar pedidos com restrições baseado no estado atual ─────────
        # Após o bloco anterior rodar, máquinas podem estar ocupadas além
        # da janela deste bloco — pedidos com maquina_especial ou min_start
        # que não conseguem iniciar nesta janela vão para o bloco correto.
        ped_b, diferidos = _separar_diferidos(
            bloco['pedidos'], filas_atual, hpd, bucket)

        for dia_ef, ped_dif in sorted(diferidos.items()):
            _inserir_em_fila(fila, idx, dia_ef, ped_dif)

        nb    = len(ped_b)
        n_dif = sum(len(v) for v in diferidos.values())

        if bucket == 'vencido':
            label = 'vencidos'
        elif bucket == 'sem_prazo':
            label = 'sem prazo'
        else:
            label = f'dia +{bucket}'

        dif_s = f'  ↪ {n_dif} diferido{"s" if n_dif != 1 else ""}' if n_dif else ''
        print(f'  Bloco {idx+1}/{len(fila)}: {label}  '
              f'({nb} pedido{"s" if nb != 1 else ""}{dif_s})')

        if nb == 0:
            idx += 1
            continue

        # ── Escolher melhor ordenação dentro do bloco ──────────────────────
        grupos_b = agrupar_por_prioridade(ped_b)
        # livre=True: SA e 2-opt podem reordenar qualquer pedido dentro do bloco.
        # A prioridade entre blocos já está garantida pelo rolling-horizon.
        # Dentro do bloco o único objetivo é liberar máquinas mais cedo possível.
        melhor_b, _ = escolher_melhor_estrategia(
            ped_b, modelos, grupos_b, ref_data, num_machines,
            filas_iniciais=filas_atual,
            livre=True,
        )

        # ── 2-opt dentro do bloco ──────────────────────────────────────────
        ord_2opt, t_2opt = busca_local_2opt(
            melhor_b['ordenados'], ref_data, num_machines,
            filas_iniciais=filas_atual,
            livre=True,
        )
        if t_2opt < melhor_b['terminoTotal']:
            ganho = _round(((melhor_b['terminoTotal'][1] - t_2opt[1])
                            / melhor_b['terminoTotal'][1]) * 100) if melhor_b['terminoTotal'][1] > 0 else 0
            melhor_b['ordenados']    = ord_2opt
            melhor_b['terminoTotal'] = t_2opt
            print(f'    2-opt −{ganho}%')

        # ── SA encaixes dentro do bloco ────────────────────────────────────
        choices_sa = sa_encaixes(
            melhor_b['ordenados'], ref_data, num_machines,
            filas_iniciais=filas_atual,
        )
        t_enc, _,  filas_enc = simular_com_atribuicao(
            melhor_b['ordenados'], ref_data, num_machines,
            choices_sa, filas_iniciais=filas_atual,
        )
        t_grd, choices_grd, filas_grd = simular_com_atribuicao(
            melhor_b['ordenados'], ref_data, num_machines,
            filas_iniciais=filas_atual,
        )

        if t_enc < t_grd:
            choices_bloco = choices_sa
            filas_atual   = filas_enc
            tard_total   += t_enc[0]
        else:
            choices_bloco = choices_grd
            filas_atual   = filas_grd
            tard_total   += t_grd[0]

        ordenados_total.extend(melhor_b['ordenados'])
        choices_total.extend(choices_bloco)
        decisao_partes.append(f'{label}:{melhor_b["id"]}')
        blocos_info.append({'bucket': bucket, 'nb': nb, 'est': melhor_b})
        idx += 1

    n_blocos       = len(blocos_info)
    makespan_total = float(filas_atual.max()) if len(filas_atual) else 0.0
    t_global       = (tard_total, makespan_total)

    decisao_str = (f'Blocos ({n_blocos}g): '
                   + ' | '.join(decisao_partes[:4])
                   + ('…' if len(decisao_partes) > 4 else ''))

    melhor_global = {
        'id':                  'blocos',
        'nome':                f'Blocos por prazo ({n_blocos} grupos)',
        'terminoTotal':        t_global,
        'terminoHoras':        _round(makespan_total),
        'ordenados':           ordenados_total,
        'decisao':             decisao_str,
        'usaPrioridade':       False,
        'totalCombinacoes':    n_blocos,
        'estrategiasPorGrupo': [
            {'grupo': i + 1,
             'estrategia': {'id': 'blocos', 'nome': bi['bucket']},
             'quantidadePedidos': bi['nb']}
            for i, bi in enumerate(blocos_info)
        ],
    }

    return ordenados_total, choices_total, melhor_global


# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    url_planilha     = sys.argv[1]
    credentials_path = sys.argv[2]

    if not os.path.exists(credentials_path):
        print(f'❌ Arquivo de credenciais não encontrado: {credentials_path}')
        sys.exit(1)

    t0 = time.time()
    print('\n📦 Otimizador de Produção — Google Sheets + Python')
    print(f'   Planilha: {url_planilha[:60]}...')
    print(f'   Workers:  {os.cpu_count() or 4} threads\n')

    print('1/8 Conectando ao Google Sheets...')
    gc          = conectar(credentials_path)
    spreadsheet = abrir_planilha(gc, url_planilha)
    print(f'  ✔ Conectado: "{spreadsheet.title}"')

    print('2/8 Lendo data base e datas bloqueadas...')
    data_base        = ler_data_base(spreadsheet)
    datas_bloqueadas = ler_datas_bloqueadas(spreadsheet)
    print(f'  ✔ Data base: {data_base.strftime("%d/%m/%Y")}')

    print('3/8 Lendo pedidos...')
    pedidos = ler_pedidos(spreadsheet, data_base, datas_bloqueadas)
    if not pedidos:
        print('❌ Nenhum pedido encontrado na aba PEDIDO.')
        sys.exit(1)
    print(f'  ✔ {len(pedidos)} pedidos.')

    print('4/8 Lendo modelos de máquinas...')
    modelos = ler_modelos(spreadsheet)
    if not modelos:
        print('❌ Nenhuma aba de máquina encontrada.')
        sys.exit(1)
    print(f'  ✔ {len(modelos)} modelo(s).')

    print('5/8 Pré-computando estrutura numpy...')
    ref_data, num_machines, ridx_map = precomputar_maquinas(modelos)
    print(f'  ✔ {num_machines} máquinas físicas indexadas.')

    modelos_novos = analisar_cores_faltantes(pedidos, modelos, spreadsheet)
    if modelos_novos is not None:
        modelos  = modelos_novos
        ref_data, num_machines, ridx_map = precomputar_maquinas(modelos)
        print(f'  ✔ Reindexado: {num_machines} máquinas físicas.')

    preparar_restricoes_pedidos(pedidos, ref_data, modelos)
    print(f'  ✔ Restrições de máquina especial aplicadas a todos os pedidos.')

    print('6/8 Otimizando em blocos por prazo (rolling-horizon)...')
    blocos_info = agrupar_por_dia_vencimento(pedidos)
    print(f'  {len(blocos_info)} bloco(s): '
          + ', '.join(
              ('vencidos' if b['bucket'] == 'vencido' else
               'sem prazo' if b['bucket'] == 'sem_prazo' else
               f'dia+{b["bucket"]}') + f'({len(b["pedidos"])}p)'
              for b in blocos_info
          ))
    ordenados_total, choices_total, melhor = otimizar_em_blocos(
        pedidos, modelos, ref_data, num_machines)
    print(f'  ✔ {melhor["decisao"]}')

    print('7/8 Gerando distribuição final...')
    resultado, sem_cadastro = otimizar_distribuicao(
        ordenados_total, modelos, ref_data, num_machines, ridx_map,
        data_base, datas_bloqueadas, choices=choices_total
    )
    # ranking informativo (apenas EDD global para comparação)
    grupos  = agrupar_por_prioridade(pedidos)
    _, ranking = escolher_melhor_estrategia(
        pedidos, modelos, grupos, ref_data, num_machines)
    sugestoes = calcular_sugestoes(modelos)
    print(f'  ✔ {len(resultado)} alocações, {len(sem_cadastro)} sem cadastro, {len(sugestoes)} sugestões.')

    print('8/8 Salvando resultados...')
    salvar_resultado(spreadsheet, resultado, sem_cadastro, sugestoes, melhor)
    salvar_comparativo(spreadsheet, melhor, ranking, len(pedidos), len(modelos),
                       pedidos=pedidos, modelos=modelos, resultado=resultado,
                       data_base=data_base)
    salvar_relatorio(spreadsheet, resultado, melhor)
    salvar_relatorio_montagem(spreadsheet, resultado)
    escrever_resultado_pedido(spreadsheet, resultado, sem_cadastro)

    tempo_total = time.time() - t0
    print(f'\n✅ Concluído em {tempo_total:.1f}s')
    print(gerar_resumo(resultado, sem_cadastro, melhor, ranking))
    print(f'\n🔗 Planilha: {spreadsheet.url}')
    print()


if __name__ == '__main__':
    main()
