#!/usr/bin/env python3
"""
Script principal para mapeamento de sites e formatação de resultados.

Este script integra as duas ferramentas principais:
1. SiteMapper - Realiza o crawling e mapeamento do site
2. PlanilhaFormatter - Formata os resultados para Excel com estrutura padronizada

Uso:
    python main.py map <url> [--test] [--output DIR] [--concurrent NUM] [--rate NUM]
    python main.py format <csv_file> [--output DIR] [--site_prefix NAME]
    python main.py full <url> [--test] [--output DIR] [--site_prefix NAME]

Exemplos:
    python main.py map https://tarf.economia.df.gov.br --output ./resultados
    python main.py format mapeamento.csv --site_prefix "Tribunal Administrativo de Recursos Fiscais"
    python main.py full https://tarf.economia.df.gov.br --site_prefix "Tribunal Administrativo de Recursos Fiscais"
"""

import os
import sys
import argparse
import asyncio
import logging
import shutil
from datetime import datetime
import time
from urllib.parse import urlparse

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("main.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MainApp")

# Importar módulos do projeto
try:
    from site_mapper import SiteMapper, run_mapper
    from planilha_formatter import PlanilhaFormatter
    logger.info("Módulos importados com sucesso.")
except ImportError as e:
    logger.error(f"Erro ao importar módulos necessários: {e}")
    logger.error("Verifique se os arquivos site_mapper.py e planilha_formatter.py estão no mesmo diretório.")
    sys.exit(1)

def extract_domain(url):
    """Extrai o nome de domínio de uma URL."""
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    
    # Remove www. se presente
    if domain.startswith('www.'):
        domain = domain[4:]
        
    # Pega apenas o primeiro segmento do domínio (antes do primeiro ponto)
    domain_parts = domain.split('.')
    site_name = domain_parts[0]
    
    return site_name

def setup_directories(base_dir="output", site_name=None):
    """Configura os diretórios necessários para output."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Adiciona nome do site ao diretório se disponível
    if site_name:
        output_dir = f"{base_dir}/{site_name}_{timestamp}"
    else:
        output_dir = f"{base_dir}/{timestamp}"
    
    # Criar diretórios
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(f"{output_dir}/raw", exist_ok=True)
    os.makedirs(f"{output_dir}/formatted", exist_ok=True)
    
    logger.info(f"Diretórios criados em: {output_dir}")
    return output_dir, timestamp

async def run_mapping(url, output_dir, test_mode=False, concurrent=10, rate=5):
    """
    Executa o mapeamento do site.
    
    Args:
        url: URL do site a ser mapeado
        output_dir: Diretório de saída
        test_mode: Se True, limita a quantidade de páginas para teste
        concurrent: Número máximo de requisições concorrentes
        rate: Requisições por segundo
        
    Returns:
        Caminho para o arquivo CSV gerado
    """
    logger.info(f"Iniciando mapeamento de: {url}")
    logger.info(f"Modo de teste: {'Ativado' if test_mode else 'Desativado'}")
    
    start_time = time.time()
    site_name = extract_domain(url)
    
    try:
        # Executar o mapeador
        await run_mapper(
            url=url,
            test_mode=test_mode,
            max_concurrent=concurrent,
            rate_limit=rate
        )
        
        # Procurar pelo arquivo CSV gerado na pasta logs
        csv_files = [f for f in os.listdir("logs") if f.endswith('.csv')]
        if not csv_files:
            logger.error("Nenhum arquivo CSV gerado pelo mapeador.")
            return None
            
        # Pegar o arquivo mais recente
        latest_csv = max(csv_files, key=lambda f: os.path.getmtime(os.path.join("logs", f)))
        csv_path = os.path.join("logs", latest_csv)
        
        # Copiar para o diretório de saída raw com o nome do site
        output_csv = f"{output_dir}/raw/{site_name}_mapeamento.csv"
        shutil.copy(csv_path, output_csv)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Mapeamento concluído em {elapsed_time:.2f} segundos.")
        logger.info(f"Resultados salvos em: {output_csv}")
        
        return output_csv
        
    except Exception as e:
        logger.error(f"Erro durante o mapeamento: {e}", exc_info=True)
        return None

def run_formatting(csv_path, output_dir, site_prefix=None, site_name=None):
    """
    Executa a formatação do CSV para Excel.
    
    Args:
        csv_path: Caminho para o arquivo CSV de input
        output_dir: Diretório de saída
        site_prefix: Nome do site a ser substituído por "Raiz"
        site_name: Nome do site para incluir no nome do arquivo
        
    Returns:
        Tuple com o caminho para os arquivos CSV e Excel formatados
    """
    logger.info(f"Iniciando formatação de: {csv_path}")
    if site_prefix:
        logger.info(f"Prefixo do site a substituir: {site_prefix}")
    
    start_time = time.time()
    
    try:
        # Se não temos o nome do site, tentamos extrair do caminho do CSV
        if not site_name and '_mapeamento.csv' in csv_path:
            site_name = os.path.basename(csv_path).split('_mapeamento.csv')[0]
        
        # Executa o formatador original
        formatter = PlanilhaFormatter(
            input_csv=csv_path,
            output_dir=f"{output_dir}/formatted",
            site_prefix=site_prefix
        )
        
        result = formatter.process()
        
        if not result:
            logger.error("Formatação falhou.")
            return None, None
            
        # Procurar pelos arquivos gerados
        formatted_dir = f"{output_dir}/formatted"
        csv_files = [f for f in os.listdir(formatted_dir) if f.endswith('.csv')]
        excel_files = [f for f in os.listdir(formatted_dir) if f.endswith('.xlsx')]
        
        if not csv_files or not excel_files:
            logger.error("Arquivos formatados não encontrados.")
            return None, None
            
        # Pegar os arquivos mais recentes
        latest_csv = max(csv_files, key=lambda f: os.path.getmtime(os.path.join(formatted_dir, f)))
        latest_excel = max(excel_files, key=lambda f: os.path.getmtime(os.path.join(formatted_dir, f)))
        
        # Renomear os arquivos para incluir o nome do site, se disponível
        if site_name:
            # Novos nomes dos arquivos
            new_csv_name = f"{site_name}_formatado.csv"
            new_excel_name = f"{site_name}_formatado.xlsx"
            
            # Caminhos completos para os arquivos
            original_csv = os.path.join(formatted_dir, latest_csv)
            original_excel = os.path.join(formatted_dir, latest_excel)
            
            new_csv_path = os.path.join(formatted_dir, new_csv_name)
            new_excel_path = os.path.join(formatted_dir, new_excel_name)
            
            # Renomear os arquivos
            shutil.copy(original_csv, new_csv_path)
            shutil.copy(original_excel, new_excel_path)
            
            csv_output = new_csv_path
            excel_output = new_excel_path
            
            logger.info(f"Arquivos renomeados para incluir nome do site: {site_name}")
        else:
            csv_output = os.path.join(formatted_dir, latest_csv)
            excel_output = os.path.join(formatted_dir, latest_excel)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Formatação concluída em {elapsed_time:.2f} segundos.")
        logger.info(f"CSV formatado salvo em: {csv_output}")
        logger.info(f"Excel formatado salvo em: {excel_output}")
        
        return csv_output, excel_output
        
    except Exception as e:
        logger.error(f"Erro durante a formatação: {e}", exc_info=True)
        return None, None

async def run_full_process(url, output_dir, site_prefix=None, test_mode=False, concurrent=10, rate=5):
    """
    Executa o processo completo de mapeamento e formatação.
    
    Args:
        url: URL do site a ser mapeado
        output_dir: Diretório de saída
        site_prefix: Nome do site a ser substituído por "Raiz"
        test_mode: Se True, limita a quantidade de páginas para teste
        concurrent: Número máximo de requisições concorrentes
        rate: Requisições por segundo
        
    Returns:
        Tuple com os caminhos para os arquivos gerados
    """
    logger.info(f"Iniciando processo completo para: {url}")
    
    start_time = time.time()
    site_name = extract_domain(url)
    
    # Etapa 1: Mapeamento
    csv_path = await run_mapping(url, output_dir, test_mode, concurrent, rate)
    if not csv_path:
        logger.error("Processo interrompido após falha no mapeamento.")
        return None, None
    
    # Etapa 2: Formatação
    csv_output, excel_output = run_formatting(csv_path, output_dir, site_prefix, site_name)
    if not csv_output or not excel_output:
        logger.error("Processo interrompido após falha na formatação.")
        return csv_path, None
    
    elapsed_time = time.time() - start_time
    minutes, seconds = divmod(elapsed_time, 60)
    
    logger.info(f"Processo completo concluído em {int(minutes)}m {int(seconds)}s.")
    logger.info(f"Resumo dos resultados:")
    logger.info(f"- CSV bruto: {csv_path}")
    logger.info(f"- CSV formatado: {csv_output}")
    logger.info(f"- Excel formatado: {excel_output}")
    
    return csv_path, excel_output

def parse_arguments():
    """Configura e processa os argumentos da linha de comando."""
    parser = argparse.ArgumentParser(
        description="Mapeador e Formatador de Sites",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python main.py map https://tarf.economia.df.gov.br --output ./resultados
  python main.py format mapeamento.csv --site_prefix "Tribunal Administrativo de Recursos Fiscais"
  python main.py full https://tarf.economia.df.gov.br --site_prefix "Tribunal Administrativo de Recursos Fiscais"
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Comando a executar')
    
    # Comando 'map' para mapeamento
    map_parser = subparsers.add_parser('map', help='Mapear um site')
    map_parser.add_argument('url', help='URL do site a ser mapeado')
    map_parser.add_argument('--test', action='store_true', help='Executar em modo de teste (limite de páginas)')
    map_parser.add_argument('--output', default='output', help='Diretório para salvar resultados')
    map_parser.add_argument('--concurrent', type=int, default=10, help='Requisições concorrentes')
    map_parser.add_argument('--rate', type=int, default=5, help='Requisições por segundo')
    
    # Comando 'format' para formatação
    format_parser = subparsers.add_parser('format', help='Formatar um CSV para Excel')
    format_parser.add_argument('csv_file', help='Arquivo CSV gerado pelo mapeador')
    format_parser.add_argument('--output', default='output', help='Diretório para salvar resultados')
    format_parser.add_argument('--site_prefix', help='Nome do site a substituir por "Raiz"')
    format_parser.add_argument('--site_name', help='Nome do site para incluir no arquivo')
    
    # Comando 'full' para o processo completo
    full_parser = subparsers.add_parser('full', help='Executar mapeamento e formatação em sequência')
    full_parser.add_argument('url', help='URL do site a ser mapeado')
    full_parser.add_argument('--test', action='store_true', help='Executar em modo de teste (limite de páginas)')
    full_parser.add_argument('--output', default='output', help='Diretório para salvar resultados')
    full_parser.add_argument('--site_prefix', help='Nome do site a substituir por "Raiz"')
    full_parser.add_argument('--concurrent', type=int, default=10, help='Requisições concorrentes')
    full_parser.add_argument('--rate', type=int, default=5, help='Requisições por segundo')
    
    return parser.parse_args()

async def main_async():
    """Função principal assíncrona."""
    args = parse_arguments()
    
    if not args.command:
        logger.error("Nenhum comando especificado. Use 'map', 'format' ou 'full'.")
        sys.exit(1)
    
    # Extrair nome do site para o diretório, se for um comando que usa URL
    site_name = None
    if args.command in ['map', 'full']:
        site_name = extract_domain(args.url)
    elif args.command == 'format' and hasattr(args, 'site_name') and args.site_name:
        site_name = args.site_name
        
    # Criar diretórios de saída com nome do site
    output_dir, timestamp = setup_directories(args.output, site_name)
    
    if args.command == 'map':
        # Executar apenas o mapeamento
        csv_path = await run_mapping(
            url=args.url,
            output_dir=output_dir,
            test_mode=args.test,
            concurrent=args.concurrent,
            rate=args.rate
        )
        if csv_path:
            logger.info(f"Mapeamento concluído com sucesso. Resultados em: {csv_path}")
        else:
            logger.error("Mapeamento falhou.")
            sys.exit(1)
            
    elif args.command == 'format':
        # Executar apenas a formatação
        site_name_arg = args.site_name if hasattr(args, 'site_name') else None
        csv_output, excel_output = run_formatting(
            csv_path=args.csv_file,
            output_dir=output_dir,
            site_prefix=args.site_prefix,
            site_name=site_name_arg
        )
        if csv_output and excel_output:
            logger.info(f"Formatação concluída com sucesso.")
            logger.info(f"Excel formatado: {excel_output}")
        else:
            logger.error("Formatação falhou.")
            sys.exit(1)
            
    elif args.command == 'full':
        # Executar o processo completo
        csv_path, excel_output = await run_full_process(
            url=args.url,
            output_dir=output_dir,
            site_prefix=args.site_prefix,
            test_mode=args.test,
            concurrent=args.concurrent,
            rate=args.rate
        )
        if csv_path and excel_output:
            logger.info(f"Processo completo concluído com sucesso.")
            logger.info(f"Excel final: {excel_output}")
        else:
            logger.error("Processo completo falhou.")
            sys.exit(1)

def main():
    """Função principal."""
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Programa interrompido pelo usuário.")
    except Exception as e:
        logger.error(f"Erro durante a execução: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()