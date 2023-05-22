# Importando Bibliotecas
import pandas as pd
from datetime import datetime
import os
import numpy as np
import locale
import sys

locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')

# Definindo variaveis
main = sys.argv[1]
os.makedirs(f'{main}\\Extratos', exist_ok=True)

p_arquivo = f'{main}\\Arquivos'
p_referencia = f'{p_arquivo}\\Referencia'
p_extratos = f'{main}\\Extratos'
p_historico = f'{p_extratos}\\ADDE_Extrato_Historico.xlsx'
p_anual = f'{p_extratos}\\2023\\ADDE_Extrato_Anual.xlsx'
p_apoio = f'{p_arquivo}\\Apoio'

# Importando arquivo fonte, primeiros tratamentos e exportando para xlsx

extrato = pd.DataFrame()
for arquivo in os.listdir(path=p_arquivo):
    try:
        if '.xls' not in arquivo and '.xlsx' not in arquivo:
            continue
        arq = pd.read_excel(f"{p_arquivo}\\{arquivo}", skiprows=[0, 1]).reset_index(drop=True)
        arq.drop([0, 1], inplace=True)
        arq.replace(np.nan, 'NULO', inplace=True)
        arq.drop(arq[arq['HISTÓRICO'].str.contains('SALDO', na=False)].index, inplace=True)
        extrato = pd.concat([extrato, arq])
        os.replace(f"{p_arquivo}\\{arquivo}", f"{p_arquivo}\\Passado\\{arquivo}")
    except:
        raise
extrato.reset_index(drop=True, inplace=True)
extrato.to_excel(f'{p_apoio}\\extrato_bruto.xlsx', index=False)

# Estruturando arquivo fonte

extrato = pd.read_excel(f'{p_apoio}\\extrato_bruto.xlsx')
extrato_processado = pd.DataFrame(columns=['DATA', 'DOCUMENTO', 'TIPO', 'EMPRESA', 'CNPJ_DEPOSITANTE', 'VALOR'])

for index in range(len(extrato)):
    # Filtrando por data e tipo de transacao 'C'= Credito
    if extrato.iloc[index]['VALOR'][-1] in 'C':
        if extrato.iloc[index]['DOCUMENTO'] == 'Pix':
            # Tratando transações do tipo PIX
            if extrato.iloc[index + 2]['HISTÓRICO'].startswith(('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')):
                empresa = 'NAO_ENCONTRADO'
                cnpj = extrato.iloc[index + 2]['HISTÓRICO']
            else:
                empresa = extrato.iloc[index + 2]['HISTÓRICO']
                cnpj = extrato.iloc[index + 3]['HISTÓRICO']
            transacao = {
                'DATA': extrato.iloc[index]['DATA'],
                'DOCUMENTO': extrato.iloc[index]['DOCUMENTO'],
                'TIPO': extrato.iloc[index]['HISTÓRICO'],
                'EMPRESA': empresa,
                'CNPJ_DEPOSITANTE': cnpj,
                'VALOR': str(extrato.iloc[index]['VALOR'])[:-1]
            }
            transacao = pd.DataFrame([transacao])
            extrato_processado = pd.concat([extrato_processado, transacao])
        else:
            # Tratando transações do tipo TED
            try:
                transacao = {
                    'DATA': extrato.iloc[index]['DATA'],
                    'DOCUMENTO': extrato.iloc[index]['DOCUMENTO'],
                    'TIPO': extrato.iloc[index]['HISTÓRICO'],
                    'EMPRESA': extrato.iloc[index + 1]['HISTÓRICO'],
                    'CNPJ_DEPOSITANTE': extrato.iloc[index + 2]['HISTÓRICO'],
                    'VALOR': str(extrato.iloc[index]['VALOR'])[:-1]
                }
            except IndexError:
                transacao = {
                    'DATA': extrato.iloc[index]['DATA'],
                    'DOCUMENTO': extrato.iloc[index]['DOCUMENTO'],
                    'TIPO': extrato.iloc[index]['HISTÓRICO'],
                    'EMPRESA': 'NAO_ENCONTRADO',
                    'CNPJ_DEPOSITANTE': 'NAO_ENCONTRADO',
                    'VALOR': str(extrato.iloc[index]['VALOR'])[:-1]
                }
            transacao = pd.DataFrame([transacao])
            extrato_processado = pd.concat([extrato_processado, transacao])

# Tratando a coluna data
extrato_processado['DATA'] = pd.to_datetime(extrato_processado['DATA'], format='%d/%m/%Y')

# Tratando a coluna VALOR
extrato_processado['VALOR'] = extrato_processado['VALOR'].str.replace('NUL', '0')
extrato_processado['VALOR'] = extrato_processado['VALOR'].str.replace('.', '')
extrato_processado['VALOR'] = extrato_processado['VALOR'].str.replace(',', '.').astype(float)

# Filtrando dados para encontrar apenas transações de CNPJ
extrato_processado['CNPJ_DEPOSITANTE'] = extrato_processado['CNPJ_DEPOSITANTE'].str.replace(' ', '/')
extrato_processado = extrato_processado[~extrato_processado['CNPJ_DEPOSITANTE'].str.startswith('***')]
extrato_processado = extrato_processado[~extrato_processado['EMPRESA'].str.startswith('***')]
extrato_processado = extrato_processado[~extrato_processado['TIPO'].str.contains('CRÉD.TRANSF.CONTAS')]

# Gerando PK para filtrar duplicados
extrato_processado['PK'] = [f'{i}{datetime.now().strftime("%b%y").upper()}' for i in range(len(extrato_processado))]

# Reset Index
extrato_processado.reset_index(drop=True, inplace=True)

# Exportando para excel
extrato_processado.to_excel(f'{p_apoio}\\extrato_processado.xlsx', index=False)

# Importando dados de CNPJ de referencia
cnpj_ref = pd.read_csv(f"{p_referencia}\\Referencia_CNPJ.csv", delimiter=';')

# Tratando colunas de CNPJ
cnpj_ref['CNPJ_DEPOSITANTE'] = cnpj_ref['CNPJ_DEPOSITANTE'].str.replace(' ', '/')
cnpj_ref['CNPJ_EMISSAO'] = cnpj_ref['CNPJ_EMISSAO'].str.replace(' ', '/')
cnpj_ref.drop_duplicates(subset='CNPJ_DEPOSITANTE', inplace=True)

# Merge de CNPJ de referencia com transações
cnpj = pd.merge(extrato_processado, cnpj_ref[['CNPJ_DEPOSITANTE', 'CNPJ_EMISSAO', 'RAZAO_SOCIAL', 'COD_EMP']],
                on=['CNPJ_DEPOSITANTE'], how='left')

# Importando codigo da empresa
cnpj['COD_EMP'].fillna('NAO_ENCONTRADO', inplace=True)

# Preenchendo nome de empresas nao encontrados
cnpj['RAZAO_SOCIAL'].fillna(cnpj['EMPRESA'], inplace=True)
cnpj['EMPRESA'] = cnpj['RAZAO_SOCIAL']
cnpj.drop(columns='RAZAO_SOCIAL', axis=1, inplace=True)

# Preenchendo CNPJ nao encontrado
cnpj['CNPJ_EMISSAO'].fillna('NAO_ENCONTRADO', inplace=True)
# Drop duplicados
cnpj.drop_duplicates(subset='PK', inplace=True)

# Filtrando tipos de transferencia
filtro_tipo = ("EST.DÉB.TRANSF.CONTAS MESMA TITULARIDADE",
               "FAV.: VILMAR GONCALVES CERQUEIRA",
               "CRÉDITO-DEVOLUÇÃO TED DIF.TITULARIDADE",
               "ESTORNO PIX EMITIDO",
               "CRÉD.DISTRIBUIÇÃO SOBRAS/VALORES",
               "TRANSF.RECEBIDA - PIX SICOOB")
cnpj.query("TIPO not in @filtro_tipo", inplace=True)

# Limpando dados do campo empresa
cnpj['EMPRESA'] = cnpj['EMPRESA'].str.replace('.', '')
cnpj['EMPRESA'] = cnpj['EMPRESA'].str.replace(':', '')

# Inserindo dados na planilha historica
try:
    with pd.ExcelWriter(path=p_historico, mode='a', if_sheet_exists='overlay') as writer:
        df = pd.read_excel(writer)
        try:
            df['DATA'] = pd.to_datetime(df['DATA'], format='%d/%m/%Y')
            df.drop(df[(df['DATA'] >= cnpj['DATA'].min()) & (df['DATA'] <= cnpj['DATA'].max())].index, inplace=True)
            print(f"Reescrevendo dados a partir de {cnpj['DATA'].min()} até {cnpj['DATA'].max()} ")
        except KeyError:
            print(f"Arquivo Vazio, preenchendo com dados de arquivo fonte a partir da data {cnpj['DATA'].min()}")
        df = pd.concat([cnpj, df], ignore_index=True)
        df.drop(columns='COD_EMP', axis=1, inplace=True)
        df.drop_duplicates(inplace=True)
        df.sort_values(by='DATA', inplace=True)
        df.to_excel(writer,
                    columns=['DATA', 'DOCUMENTO', 'TIPO', 'EMPRESA', 'CNPJ_DEPOSITANTE', 'CNPJ_EMISSAO', 'VALOR'],
                    index=False,
                    sheet_name='Historico')
except FileNotFoundError:
    cnpj.to_excel(p_historico,
                  columns=['DATA', 'DOCUMENTO', 'TIPO', 'EMPRESA', 'CNPJ_DEPOSITANTE', 'CNPJ_EMISSAO', 'VALOR'],
                  index=False,
                  sheet_name='Historico')

# Extrato Anual
for y in pd.to_datetime(cnpj['DATA'], format='%d/%m/%Y').dt.year.drop_duplicates():
    extrato_anual = cnpj[pd.to_datetime(cnpj['DATA'], format='%d/%m/%Y').dt.year == y]

    # Extrato Mensal
    for m in pd.to_datetime(extrato_anual['DATA'], format='%d/%m/%Y').dt.month.drop_duplicates():
        extrato_mensal = extrato_anual[pd.to_datetime(extrato_anual['DATA'], format='%d/%m/%Y').dt.month == m]
        ano = pd.to_datetime(extrato_mensal['DATA'].min(), format='%d/%m/%Y').year
        mes = pd.to_datetime(extrato_mensal['DATA'].min(), format='%d/%m/%Y').strftime('%B').capitalize()
        os.makedirs(f"{p_extratos}\\{ano}\\{mes}", exist_ok=True)
        excel_mensal = extrato_mensal
        saldo = {
            'DATA': '',
            'DOCUMENTO': '',
            'TIPO': '',
            'EMPRESA': '',
            'CNPJ_DEPOSITANTE': 'SALDO',
            'VALOR': excel_mensal['VALOR'].sum()
        }
        saldo = pd.DataFrame([saldo])
        excel_mensal = pd.concat([excel_mensal, saldo])
        excel_mensal.to_excel(f"{p_extratos}\\{ano}\\{mes}\\ADDE_Extrato_Mensal_{mes}_{ano}.xlsx",
                              columns=['DATA', 'DOCUMENTO', 'TIPO', 'EMPRESA', 'CNPJ_DEPOSITANTE', 'CNPJ_EMISSAO',
                                       'VALOR'],
                              index=False)
        relatorio_mensal = pd.DataFrame()

        # Gerando Extrato mensal por empresa
        for empresa in extrato_mensal['COD_EMP'].drop_duplicates():
            df_emissao = extrato_mensal[extrato_mensal['COD_EMP'] == empresa]
            if empresa == 'NAO_ENCONTRADO':
                operadora = 'CNPJ_NAO_ENCONTRADO'
            else:
                operadora = extrato_mensal[extrato_mensal['COD_EMP'] == empresa].iloc[0]['EMPRESA'].replace(' ', '_')
            saldo = {
                'DATA': '',
                'DOCUMENTO': '',
                'TIPO': '',
                'EMPRESA': '',
                'CNPJ_DEPOSITANTE': 'SALDO',
                'VALOR': extrato_mensal[extrato_mensal['COD_EMP'] == empresa]['VALOR'].sum()
            }
            saldo = pd.DataFrame([saldo])
            df_emissao = pd.concat([df_emissao, saldo])
            relatorio_mensal = pd.concat([relatorio_mensal, df_emissao])
            df_emissao.to_excel(f"{p_extratos}\\{ano}\\{mes}\\{operadora}.xlsx",
                                columns=['PK', 'DATA', 'DOCUMENTO', 'TIPO', 'EMPRESA', 'CNPJ_DEPOSITANTE',
                                         'CNPJ_EMISSAO', 'VALOR'],
                                index=False)
            print(
                fr'Relatório de {mes} de {ano} - {operadora}.xlsx criado no caminho C:\Users\joaoc\Documents\Adde\Extratos\2023\Abril\{operadora}.xlsx')
        relatorio_mensal.to_excel(f"{p_extratos}\\{ano}\\{mes}\\Relatorio_Mensal_{mes}.xlsx",
                                  columns=['DATA', 'DOCUMENTO', 'TIPO', 'EMPRESA', 'CNPJ_DEPOSITANTE', 'CNPJ_EMISSAO',
                                           'VALOR'],
                                  index=False)
