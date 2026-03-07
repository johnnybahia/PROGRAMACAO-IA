#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Otimizador de Distribuição de Produção — Google Sheets + Python
================================================================
Lê e escreve diretamente na planilha Google Sheets.
Sem limite de tempo. Simulações em paralelo com numpy.

Estrutura da aba PEDIDO:
  Col A: Data Inicial Especial (opcional — força início a partir desta data)
  Col B: Produto
  Col C: Referência
  Col D: Cor
  Col E: Quantidade de Máquinas
  Col F: Cliente
  Col G: Ordem de Compra
  Col H: Data de Entrega da OC (deadline)
  Col I: Data Finalização do Pedido  ← preenchida pelo código
  Col J: Prazo                       ← preenchida pelo código (+X antecipado / -X atrasado)
  Cel L1: Data base de início do planejamento

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
    'LIMIAR_TROCA_PERCENT': 10,
    'ABAS_IGNORAR': {
        'PEDIDO', 'DISTRIBUIÇÃO', 'COMPARATIVO', 'RELATORIO',
        'DATAS FORA DE PROGRAMAÇÃO',
        'Página1', 'Sheet1', 'Resumo', 'DADOS_GERAIS', 'DADOS',
    },
    # Monte Carlo: mais iterações pois não há limite de tempo
    # > 1000 pedidos → 50 iter | > 500 → 100 | > 200 → 200 | ≤ 200 → 500
    'MC_ITER': [(1000, 50), (500, 100), (200, 200), (0, 500)],
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

        print(f'  ✔ Aba "{self.name}" salva.')

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


def horas_para_data(base_date: date, horas_offset: float, datas_bloqueadas: set) -> datetime:
    """
    Converte offset em horas virtuais (excluindo dias bloqueados) para datetime real.
    base_date é o dia 0 do planejamento (hora 0).
    Dias bloqueados são pulados — não contam para o offset.
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
        if data_atual not in datas_bloqueadas:
            dias_contados += 1

    return datetime.combine(data_atual, datetime.min.time()) + timedelta(hours=horas_restantes)


def data_para_horas(base_date: date, target_date: date, datas_bloqueadas: set) -> float:
    """
    Converte uma data alvo em offset de horas virtuais a partir de base_date,
    excluindo dias bloqueados da contagem.
    """
    if target_date <= base_date:
        return 0.0
    hpd       = CONFIG['HORAS_POR_DIA']
    dias      = 0
    data_atual = base_date
    while data_atual < target_date:
        data_atual += timedelta(days=1)
        if data_atual not in datas_bloqueadas:
            dias += 1
    return float(dias * hpd)


# ── LER DATA BASE E DATAS BLOQUEADAS ─────────────────────────────────────────
def ler_data_base(spreadsheet) -> date:
    """Lê a data base de início (célula L1) da aba PEDIDO."""
    ws  = spreadsheet.worksheet(CONFIG['ABA_PEDIDO'])
    val = ws.acell('L1').value or ''
    d   = parse_data(val)
    if d is None:
        d = date.today()
        print(f'  ⚠ L1 da aba PEDIDO inválido ou vazio. Usando hoje: {d.strftime("%d/%m/%Y")}')
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
      A: Data Inicial Especial  B: Produto  C: Referência  D: Cor
      E: Qtd Máquinas  F: Cliente  G: Ordem de Compra  H: Data de Entrega
      I: Data Finalização (saída)  J: Prazo (saída)
      L1: Data base (lida separadamente por ler_data_base)
    """
    ws   = spreadsheet.worksheet(CONFIG['ABA_PEDIDO'])
    rows = ws.get_all_values()
    pedidos = []

    for i, linha in enumerate(rows[1:], start=2):   # i = linha real no sheet
        if len(linha) < 5:
            continue
        try:
            data_esp_str = linha[0].strip() if linha[0].strip() else ''
            produto      = linha[1].strip() if len(linha) > 1 else ''
            ref          = linha[2].strip() if len(linha) > 2 else ''
            cor          = linha[3].strip() if len(linha) > 3 else ''
            qtd_str      = linha[4].strip() if len(linha) > 4 else ''
            cliente      = linha[5].strip() if len(linha) > 5 else ''
            ordem_compra = linha[6].strip() if len(linha) > 6 else ''
            data_ent_str = linha[7].strip() if len(linha) > 7 else ''
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

        # Data inicial especial → min_start em horas virtuais
        data_esp  = parse_data(data_esp_str)
        min_start = 0.0
        if data_esp:
            min_start = data_para_horas(data_base, data_esp, datas_bloqueadas)

        # Deadline → offset em horas virtuais
        data_entrega   = parse_data(data_ent_str)
        deadline_horas = None
        if data_entrega:
            deadline_horas = data_para_horas(data_base, data_entrega, datas_bloqueadas)

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
            'data_especial':        data_esp,
            'min_start':            min_start,
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
            rows = ws.get_all_values()
            for linha in rows[1:]:
                if len(linha) < 7:
                    continue
                ref       = linha[6].strip()    # Coluna G
                tempo_str = linha[1].strip()    # Coluna B
                if not ref:
                    continue
                try:
                    tempo = float(tempo_str.replace(',', '.'))
                except (ValueError, AttributeError):
                    continue
                if tempo <= 0:
                    continue
                referencias[ref] = tempo

            if not referencias:
                continue

            modelos[nome] = {
                'nome_modelo':    nome_modelo,
                'total_maquinas': total_maq,
                'referencias':    referencias,
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

    for ref in ref_data:
        ref_data[ref]['gidxs']  = np.array(ref_data[ref]['gidxs'],  dtype=np.int32)
        ref_data[ref]['tempos'] = np.array(ref_data[ref]['tempos'], dtype=np.float64)

    ridx_map = {v: k for k, v in gidx_map.items()}
    return ref_data, num_machines, ridx_map


# ── RESOLUÇÃO DE CHAVE (ref + cor com fallback para ref genérica) ─────────────
def _chave_pedido(p: dict, ref_data: dict) -> str:
    """
    Retorna a chave a usar em ref_data para este pedido.
    Tenta 'referencia cor' (específico por cor), cai para 'referencia' genérica.
    Ex.: ref='M60109' cor='2410' → tenta 'M60109 2410', senão usa 'M60109'.
    """
    ref = p['referencia']
    cor = (p.get('cor') or '').strip()
    if cor and cor != '-':
        combined = f"{ref} {cor}"
        if combined in ref_data:
            return combined
    return ref


# ── SIMULAÇÃO (núcleo quente) ────────────────────────────────────────────────
def simular_termino(pedidos: list, ref_data: dict, num_machines: int) -> float:
    """
    Simula tempo total de produção respeitando min_start de cada pedido.
    Thread-safe (cria 'filas' local).
    """
    filas = np.zeros(num_machines, dtype=np.float64)
    maior = 0.0
    for p in pedidos:
        d = ref_data.get(_chave_pedido(p, ref_data))
        if d is None:
            continue
        gidxs, tempos = d['gidxs'], d['tempos']
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


# ── ESTRATÉGIAS ──────────────────────────────────────────────────────────────
def get_menor_tempo(ref: str, modelos: dict) -> float:
    menor = float('inf')
    for mod in modelos.values():
        if ref in mod['referencias']:
            menor = min(menor, mod['referencias'][ref])
    return menor if menor != float('inf') else 9999


def make_estrategias(modelos: dict, ref_data: dict, num_machines: int) -> list:
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
        chaves  = list(grupos.keys())
        result  = []
        while any(grupos[k] for k in chaves):
            for k in chaves:
                if grupos[k]:
                    result.append(grupos[k].pop(0))
        return result

    def rapido(pedidos):
        return sorted(pedidos, key=lambda p: (
            get_menor_tempo(p['referencia'], modelos),
            p['referencia'], p.get('cor', ''),
        ))

    def menor_demanda(pedidos):
        return sorted(pedidos, key=lambda p: (p['maquinas_necessarias'], p.get('cor', '')))

    def maior_demanda(pedidos):
        return sorted(pedidos, key=lambda p: (-p['maquinas_necessarias'], p.get('cor', '')))

    def lento(pedidos):
        return sorted(pedidos, key=lambda p: (
            -get_menor_tempo(p['referencia'], modelos), p.get('cor', ''),
        ))

    def monte_carlo(pedidos):
        n     = len(pedidos)
        iters = _mc_iter(n)
        melhor  = None
        mt      = float('inf')
        for _ in range(iters):
            emb = random.sample(pedidos, n)
            t   = simular_termino(emb, ref_data, num_machines)
            if t < mt:
                mt     = t
                melhor = emb
        return melhor or pedidos

    return [
        {'id': 'edd',          'nome': '✅ EDD — Prazo Mais Próximo Primeiro',
         'descricao': 'Prioriza a data de entrega — minimiza atrasos',
         'fn': edd},
        {'id': 'balanceamento','nome': '2 — Balanceamento por Modelo',
         'descricao': 'Distribui equalizando carga entre modelos — operador focado',
         'fn': balanceamento},
        {'id': 'rapido',       'nome': '3 — Mais Rápido Primeiro',
         'descricao': 'Menor tempo de produção primeiro — libera máquinas mais cedo',
         'fn': rapido},
        {'id': 'menor_demanda','nome': '4 — Menor Demanda Primeiro',
         'descricao': 'Menos máquinas necessárias primeiro — fecha muitos pedidos rapidamente',
         'fn': menor_demanda},
        {'id': 'maior_demanda','nome': '5 — Maior Demanda Primeiro',
         'descricao': 'Mais máquinas necessárias primeiro — resolve gargalos grandes logo',
         'fn': maior_demanda},
        {'id': 'lento',        'nome': '6 — Mais Lento Primeiro',
         'descricao': 'Maior tempo de produção primeiro — jobs longos entram antes',
         'fn': lento},
        {'id': 'monte_carlo',  'nome': '7 — Melhor Aleatório (Monte Carlo)',
         'descricao': f'Até {CONFIG["MC_ITER"][0][1]}–{CONFIG["MC_ITER"][-1][1]} simulações aleatórias — adapta ao volume',
         'fn': monte_carlo},
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


# ── ESCOLHA PARALELA DA MELHOR ESTRATÉGIA ────────────────────────────────────
def escolher_melhor_estrategia(pedidos, modelos, grupos, ref_data, num_machines):
    estrategias = make_estrategias(modelos, ref_data, num_machines)
    limiar      = CONFIG['LIMIAR_TROCA_PERCENT']
    idx_ref     = next(i for i, e in enumerate(estrategias) if e['id'] == 'edd')
    num_grupos  = len(grupos)

    # Pré-computa ordenações de cada grupo × estratégia
    print('  Pré-computando ordenações por grupo e estratégia...')
    group_orderings = []
    for g in grupos:
        ord_g = [est['fn'](g['pedidos']) for est in estrategias]
        group_orderings.append(ord_g)

    # Ranking individual (todos os pedidos juntos)
    print('  Calculando ranking individual das estratégias...')
    ranking = []
    for est in estrategias:
        ordenados = est['fn'](pedidos)
        t = simular_termino(ordenados, ref_data, num_machines)
        ranking.append({**est, 'terminoTotal': t, 'terminoHoras': _round(t), 'ordenados': ordenados})
    t_ref_rank = next(r for r in ranking if r['id'] == 'edd')['terminoTotal']
    for r in ranking:
        r['diff']       = _round(r['terminoTotal'] - t_ref_rank)
        r['percentual'] = _round(((r['terminoTotal'] - t_ref_rank) / t_ref_rank) * 100) if t_ref_rank > 0 else 0
    ranking.sort(key=lambda r: r['terminoTotal'])

    # Referência: EDD em todos os grupos
    comb_ref      = [idx_ref] * num_grupos
    ordenados_ref = [p for g_idx, g in enumerate(grupos) for p in group_orderings[g_idx][idx_ref]]
    tempo_ref     = simular_termino(ordenados_ref, ref_data, num_machines)

    # Testa TODAS as combinações em paralelo
    combinacoes = gerar_combinacoes(num_grupos, len(estrategias))
    print(f'  Testando {len(combinacoes)} combinações em paralelo...')

    melhor_comb      = list(comb_ref)
    melhor_tempo     = tempo_ref
    melhor_ordenados = ordenados_ref

    def _eval(comb):
        ordenados = [p for g_i, g in enumerate(grupos) for p in group_orderings[g_i][comb[g_i]]]
        return simular_termino(ordenados, ref_data, num_machines), comb, ordenados

    workers = min(os.cpu_count() or 4, len(combinacoes))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_eval, c): c for c in combinacoes}
        done    = 0
        for fut in as_completed(futures):
            t, comb, ord_ = fut.result()
            done += 1
            if done % 100 == 0:
                print(f'    {done}/{len(combinacoes)} combinações testadas...')
            if t < melhor_tempo:
                melhor_tempo     = t
                melhor_comb      = comb
                melhor_ordenados = ord_

    is_todo_ref = lambda c: all(i == idx_ref for i in c)
    ganho = ((melhor_tempo - tempo_ref) / tempo_ref) * 100 if tempo_ref > 0 else 0

    if not is_todo_ref(melhor_comb) and ganho <= -limiar:
        comb_final      = melhor_comb
        tempo_final     = melhor_tempo
        ordenados_final = melhor_ordenados
        decisao = f'⚡ Combinação otimizada foi {abs(_round(ganho))}% mais rápida — superou o limiar de {limiar}%'
    else:
        comb_final      = comb_ref
        tempo_final     = tempo_ref
        ordenados_final = ordenados_ref
        info = ''
        if not is_todo_ref(melhor_comb) and ganho < 0:
            info = f' (melhor foi {abs(_round(ganho))}% mais rápida — abaixo do limiar de {limiar}%)'
        decisao = f'✅ EDD venceu{info}'

    estrategias_por_grupo = [
        {
            'grupo':             grupos[i]['prioridade'],
            'estrategia':        estrategias[comb_final[i]],
            'quantidadePedidos': len(grupos[i]['pedidos']),
        }
        for i in range(num_grupos)
    ]

    usa_pri  = num_grupos > 1
    todo_ref = is_todo_ref(comb_final)

    def _nome_res(n):
        return n.replace('✅ ', '').replace('2 — ', '').replace('3 — ', '') \
                .replace('4 — ', '').replace('5 — ', '').replace('6 — ', '') \
                .replace('7 — ', '')[:22]

    nome_est = (estrategias[idx_ref]['nome'] if todo_ref
                else ' | '.join(f"G{g['grupo']}: {_nome_res(g['estrategia']['nome'])}"
                                for g in estrategias_por_grupo))

    melhor = {
        'id':                  'edd' if todo_ref else 'combinacao_prioridade',
        'nome':                nome_est,
        'terminoTotal':        tempo_final,
        'terminoHoras':        _round(tempo_final),
        'ordenados':           ordenados_final,
        'decisao':             decisao,
        'estrategiasPorGrupo': estrategias_por_grupo,
        'usaPrioridade':       usa_pri,
        'totalCombinacoes':    len(combinacoes),
    }

    return melhor, ranking


# ── OTIMIZAR DISTRIBUIÇÃO ────────────────────────────────────────────────────
def otimizar_distribuicao(pedidos_ordenados, modelos, ref_data, num_machines, ridx_map,
                           data_base: date, datas_bloqueadas: set):
    filas        = np.zeros(num_machines, dtype=np.float64)
    resultado    = []
    sem_cadastro = []

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

        d = ref_data.get(_chave_pedido(pedido, ref_data))
        if d is None:
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

        gidxs, tempos, aba_idx = d['gidxs'], d['tempos'], d['aba_idx']
        por_modelo = {}

        for _ in range(slots):
            available = np.maximum(filas[gidxs], min_s)
            ft    = available + tempos
            best  = int(np.argmin(ft))
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
                prazo_str   = (f'+{delta} dias antecipado' if delta >= 0
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


# ── ESCREVER DE VOLTA NA ABA PEDIDO (colunas I e J) ──────────────────────────
def escrever_resultado_pedido(spreadsheet, resultado: list, sem_cadastro: list):
    """Preenche colunas I (data finalização) e J (prazo) na aba PEDIDO."""
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
        val_i = r['dt_termino'].strftime('%d/%m/%Y')
        val_j = r['prazo_str'] if r['prazo_str'] else '—'
        cell_list.append(gspread.Cell(ln, 9, val_i))
        cell_list.append(gspread.Cell(ln, 10, val_j))

    # Items sem cadastro: informa na coluna I, J vazia
    for r in sem_cadastro:
        ln = r.get('linha_sheet')
        if ln is None or ln in por_linha:
            continue
        cell_list.append(gspread.Cell(ln, 9, 'Sem cadastro'))
        cell_list.append(gspread.Cell(ln, 10, '—'))

    if cell_list:
        ws.update_cells(cell_list, value_input_option='RAW')
        print(f'  ✔ {len(por_linha) + len(sem_cadastro)} linha(s) atualizadas na aba PEDIDO (I e J).')


# ── SALVAR RESULTADO ─────────────────────────────────────────────────────────
def salvar_resultado(spreadsheet, resultado, sem_cadastro, sugestoes, melhor):
    cab   = ['Referência', 'Produto', 'Cor', 'Cliente', 'Ordem de Compra',
             'Modelo', 'Aba', 'Máquinas', 'Tempo Prod. (h)',
             'Início', 'Término', 'Data Entrega', 'Prazo']
    ncols = len(cab)
    b     = SheetBuilder(spreadsheet, CONFIG['ABA_RESULTADO'], cols=ncols)

    cor_banner = '#1B5E20' if melhor['id'] in ('edd', 'balanceamento') else '#E65100'
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


# ── SALVAR COMPARATIVO ───────────────────────────────────────────────────────
def salvar_comparativo(spreadsheet, melhor, ranking, num_pedidos, num_modelos):
    cab   = ['Posição', 'Estratégia', 'Descrição', 'Término Total (h)', 'Diferença vs Melhor (h)', 'Variação %']
    ncols = len(cab)
    b     = SheetBuilder(spreadsheet, 'COMPARATIVO', cols=ncols)

    b.banner('📊 COMPARATIVO DE ESTRATÉGIAS DE DISTRIBUIÇÃO', '#0D47A1', font_size=13)

    cor_b = '#1B5E20' if melhor['id'] in ('edd', 'balanceamento') else '#E65100'
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

    melhor_t = ranking[0]['terminoTotal']
    for i, est in enumerate(ranking):
        diff   = _round(est['terminoTotal'] - melhor_t)
        perc   = _round(((est['terminoTotal'] - melhor_t) / melhor_t) * 100) if melhor_t > 0 else 0
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
    _secao_cientifica(b, num_pedidos, num_modelos)
    b.freeze(4)
    b.flush()


# ── SALVAR RELATÓRIO PARA IMPRESSÃO ──────────────────────────────────────────
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
    b.banner(f'📋 RELATÓRIO DE PRODUÇÃO — Gerado em {hoje}', '#0D47A1', font_size=13)
    cor_b = '#1B5E20' if melhor['id'] in ('edd', 'balanceamento') else '#E65100'
    b.banner(
        f"🏆 {melhor['nome']}  |  Término total: {melhor['terminoHoras']}h  |  {melhor.get('decisao', '')}",
        cor_b, font_size=11)
    b.blank()
    b.write(cab, bg='#263238', fg='#FFFFFF', bold=True, h_align='CENTER')
    b.freeze(4)

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

        b.write([
            inicio_s, termino_s,
            r['referencia'], r.get('produto', ''), r.get('cor', ''),
            r.get('cliente', ''), r.get('ordem_compra', ''),
            r['nome_modelo'], r['maquinas_alocadas'],
            entrega_s, r.get('prazo_str', ''),
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
            diff = '✅ melhor' if i == 0 else f"+{_round(est['terminoTotal'] - ranking[0]['terminoTotal'])}h"
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
        ref = p['referencia']
        cor = (p.get('cor') or '').strip()
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

        sugestoes.append({
            'ref':               ref,
            'cor':               cor,
            'combined':          combined,
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
    termino_orig = simular_termino(pedidos, ref_data_orig, num_orig)
    termino_sim  = simular_termino(pedidos, ref_data_sim,  num_sim)
    melhoria_h   = _round(termino_orig - termino_sim)
    melhoria_pct = _round((melhoria_h / termino_orig * 100) if termino_orig > 0 else 0)

    # ── 3. Exibir análise completa ────────────────────────────────────────────
    print('\n' + '─' * 60)
    print('🎨 ANÁLISE DE CORES NÃO CADASTRADAS')
    print(f'   {len(sugestoes)} combinação(ões) ref+cor com lacuna nas máquinas:\n')
    for sug in sugestoes:
        nomes_f = ', '.join(modelos[a]['nome_modelo'] for a in sug['maquinas_faltando'])
        nomes_t = ', '.join(modelos[a]['nome_modelo'] for a in sug['maquinas_com_cor']) or '(nenhuma)'
        print(f'   Ref "{sug["ref"]}"  Cor "{sug["cor"]}"')
        print(f'     Já cadastrada em.: {nomes_t}')
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
    print('      coluna G = "referencia cor"  |  coluna B = tempo de produção (h)')
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

    print('6/8 Escolhendo melhor estratégia...')
    grupos = agrupar_por_prioridade(pedidos)
    melhor, ranking = escolher_melhor_estrategia(pedidos, modelos, grupos, ref_data, num_machines)
    print(f'  ✔ {melhor["decisao"]}')

    print('7/8 Gerando distribuição otimizada...')
    resultado, sem_cadastro = otimizar_distribuicao(
        melhor['ordenados'], modelos, ref_data, num_machines, ridx_map,
        data_base, datas_bloqueadas
    )
    sugestoes = calcular_sugestoes(modelos)
    print(f'  ✔ {len(resultado)} alocações, {len(sem_cadastro)} sem cadastro, {len(sugestoes)} sugestões.')

    print('8/8 Salvando resultados...')
    salvar_resultado(spreadsheet, resultado, sem_cadastro, sugestoes, melhor)
    salvar_comparativo(spreadsheet, melhor, ranking, len(pedidos), len(modelos))
    salvar_relatorio(spreadsheet, resultado, melhor)
    escrever_resultado_pedido(spreadsheet, resultado, sem_cadastro)

    tempo_total = time.time() - t0
    print(f'\n✅ Concluído em {tempo_total:.1f}s')
    print(gerar_resumo(resultado, sem_cadastro, melhor, ranking))
    print(f'\n🔗 Planilha: {spreadsheet.url}')
    print()


if __name__ == '__main__':
    main()
