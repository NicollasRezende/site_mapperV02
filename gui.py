#!/usr/bin/env python3
"""
Interface gráfica para o Mapeador e Formatador de Sites

Esta interface permite:
1. Mapear sites através de uma interface amigável
2. Formatar os resultados do mapeamento
3. Executar o processo completo de mapeamento e formatação
4. Visualizar logs e status do processo
"""

import os
import sys
import asyncio
import threading
import queue
import logging
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
from datetime import datetime
import webbrowser
import re

# Configuração do logger específico para a GUI
logger = logging.getLogger("GUI")
logger.setLevel(logging.INFO)

# Formatador de logs
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Handler para arquivo
file_handler = logging.FileHandler("gui.log")
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

# Queue para comunicação entre threads
log_queue = queue.Queue()

class QueueHandler(logging.Handler):
    """Handler personalizado para enviar logs para uma queue"""
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(record)

# Adicionar o handler de queue
queue_handler = QueueHandler(log_queue)
logger.addHandler(queue_handler)

class MapeadorGUI(tk.Tk):
    """Classe principal da interface gráfica"""
    
    def __init__(self):
        super().__init__()
        
        # Configuração da janela principal
        self.title("Mapeador e Formatador de Sites")
        self.geometry("900x650")
        self.minsize(800, 600)
        
        # Variáveis de controle
        self.url_var = tk.StringVar()
        self.output_dir_var = tk.StringVar(value=os.path.join(os.getcwd(), "output"))
        self.site_prefix_var = tk.StringVar()
        self.test_mode_var = tk.BooleanVar(value=False)
        self.csv_file_var = tk.StringVar()
        self.concurrent_var = tk.IntVar(value=10)
        self.rate_var = tk.IntVar(value=5)
        self.running = False
        self.process = None
        
        # Campos de resultado
        self.raw_csv_path = None
        self.excel_output_path = None
        
        # Criar widgets
        self.create_menu()
        self.create_main_frame()
        self.create_tabs()
        self.create_log_area()
        self.create_status_bar()
        
        # Configurar processador de logs
        self.after(100, self.process_log_queue)
        
        # Registrar função para encerrar o programa corretamente
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Registrar roots dos widgets para temas
        self.style = ttk.Style()
        self.set_theme("default")
        
        logger.info("Interface gráfica iniciada")
        
    def create_menu(self):
        """Criar menu da aplicação"""
        menu_bar = tk.Menu(self)
        
        # Menu Arquivo
        file_menu = tk.Menu(menu_bar, tearoff=0)
        file_menu.add_command(label="Abrir CSV...", command=self.browse_csv)
        file_menu.add_command(label="Selecionar pasta de saída...", command=self.browse_output_dir)
        file_menu.add_separator()
        file_menu.add_command(label="Sair", command=self.on_closing)
        menu_bar.add_cascade(label="Arquivo", menu=file_menu)
        
        # Menu Executar
        run_menu = tk.Menu(menu_bar, tearoff=0)
        run_menu.add_command(label="Mapear site", command=lambda: self.prepare_execution("map"))
        run_menu.add_command(label="Formatar CSV", command=lambda: self.prepare_execution("format"))
        run_menu.add_command(label="Processo completo", command=lambda: self.prepare_execution("full"))
        run_menu.add_separator()
        run_menu.add_command(label="Parar processo", command=self.stop_process)
        menu_bar.add_cascade(label="Executar", menu=run_menu)
        
        # Menu Resultados
        results_menu = tk.Menu(menu_bar, tearoff=0)
        results_menu.add_command(label="Abrir CSV bruto", command=lambda: self.open_result_file(self.raw_csv_path))
        results_menu.add_command(label="Abrir Excel formatado", command=lambda: self.open_result_file(self.excel_output_path))
        results_menu.add_command(label="Abrir pasta de resultados", command=self.open_output_dir)
        menu_bar.add_cascade(label="Resultados", menu=results_menu)
        
        # Menu Opções
        options_menu = tk.Menu(menu_bar, tearoff=0)
        
        # Submenu de temas
        theme_menu = tk.Menu(options_menu, tearoff=0)
        theme_menu.add_command(label="Padrão", command=lambda: self.set_theme("default"))
        theme_menu.add_command(label="Claro", command=lambda: self.set_theme("light"))
        theme_menu.add_command(label="Escuro", command=lambda: self.set_theme("dark"))
        options_menu.add_cascade(label="Tema", menu=theme_menu)
        
        options_menu.add_command(label="Limpar logs", command=self.clear_logs)
        menu_bar.add_cascade(label="Opções", menu=options_menu)
        
        # Menu Ajuda
        help_menu = tk.Menu(menu_bar, tearoff=0)
        help_menu.add_command(label="Sobre", command=self.show_about)
        help_menu.add_command(label="Documentação", command=self.show_documentation)
        menu_bar.add_cascade(label="Ajuda", menu=help_menu)
        
        self.config(menu=menu_bar)
        
    def create_main_frame(self):
        """Criar o frame principal que conterá todos os widgets"""
        self.main_frame = ttk.Frame(self)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
    def create_tabs(self):
        """Criar abas para as diferentes funcionalidades"""
        self.tab_control = ttk.Notebook(self.main_frame)
        
        # Aba de Mapeamento
        self.map_tab = ttk.Frame(self.tab_control)
        self.create_map_tab()
        self.tab_control.add(self.map_tab, text="Mapear Site")
        
        # Aba de Formatação
        self.format_tab = ttk.Frame(self.tab_control)
        self.create_format_tab()
        self.tab_control.add(self.format_tab, text="Formatar CSV")
        
        # Aba de Processo Completo
        self.full_tab = ttk.Frame(self.tab_control)
        self.create_full_tab()
        self.tab_control.add(self.full_tab, text="Processo Completo")
        
        # Aba de Configurações Avançadas
        self.advanced_tab = ttk.Frame(self.tab_control)
        self.create_advanced_tab()
        self.tab_control.add(self.advanced_tab, text="Configurações Avançadas")
        
        self.tab_control.pack(fill=tk.BOTH, expand=True)
        
    def create_map_tab(self):
        """Criar conteúdo da aba de mapeamento"""
        frame = ttk.LabelFrame(self.map_tab, text="Mapeamento de Site")
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # URL do site
        ttk.Label(frame, text="URL do site:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        url_entry = ttk.Entry(frame, textvariable=self.url_var, width=60)
        url_entry.grid(row=0, column=1, sticky=tk.W+tk.E, padx=5, pady=5, columnspan=2)
        
        # Diretório de saída
        ttk.Label(frame, text="Diretório de saída:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        output_entry = ttk.Entry(frame, textvariable=self.output_dir_var, width=50)
        output_entry.grid(row=1, column=1, sticky=tk.W+tk.E, padx=5, pady=5)
        ttk.Button(frame, text="Procurar...", command=self.browse_output_dir).grid(row=1, column=2, padx=5, pady=5)
        
        # Modo de teste
        test_check = ttk.Checkbutton(frame, text="Modo de teste (limitar quantidade de páginas)", variable=self.test_mode_var)
        test_check.grid(row=2, column=0, columnspan=3, sticky=tk.W, padx=5, pady=5)
        
        # Botão de execução
        execute_button = ttk.Button(frame, text="Iniciar Mapeamento", command=lambda: self.prepare_execution("map"))
        execute_button.grid(row=3, column=0, columnspan=3, pady=10)
        
        # Configurar expansão da coluna
        frame.columnconfigure(1, weight=1)
        
    def create_format_tab(self):
        """Criar conteúdo da aba de formatação"""
        frame = ttk.LabelFrame(self.format_tab, text="Formatação de CSV")
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Arquivo CSV de entrada
        ttk.Label(frame, text="Arquivo CSV:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        csv_entry = ttk.Entry(frame, textvariable=self.csv_file_var, width=50)
        csv_entry.grid(row=0, column=1, sticky=tk.W+tk.E, padx=5, pady=5)
        ttk.Button(frame, text="Procurar...", command=self.browse_csv).grid(row=0, column=2, padx=5, pady=5)
        
        # Diretório de saída
        ttk.Label(frame, text="Diretório de saída:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        output_entry = ttk.Entry(frame, textvariable=self.output_dir_var, width=50)
        output_entry.grid(row=1, column=1, sticky=tk.W+tk.E, padx=5, pady=5)
        ttk.Button(frame, text="Procurar...", command=self.browse_output_dir).grid(row=1, column=2, padx=5, pady=5)
        
        # Prefixo do site
        ttk.Label(frame, text="Prefixo do site:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        prefix_entry = ttk.Entry(frame, textvariable=self.site_prefix_var, width=50)
        prefix_entry.grid(row=2, column=1, sticky=tk.W+tk.E, padx=5, pady=5, columnspan=2)
        ttk.Label(frame, text="(Nome a ser substituído por 'Raiz' na hierarquia)").grid(row=3, column=1, sticky=tk.W, padx=5)
        
        # Botão de execução
        execute_button = ttk.Button(frame, text="Iniciar Formatação", command=lambda: self.prepare_execution("format"))
        execute_button.grid(row=4, column=0, columnspan=3, pady=10)
        
        # Configurar expansão da coluna
        frame.columnconfigure(1, weight=1)
        
    def create_full_tab(self):
        """Criar conteúdo da aba de processo completo"""
        frame = ttk.LabelFrame(self.full_tab, text="Processo Completo (Mapeamento + Formatação)")
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # URL do site
        ttk.Label(frame, text="URL do site:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        url_entry = ttk.Entry(frame, textvariable=self.url_var, width=60)
        url_entry.grid(row=0, column=1, sticky=tk.W+tk.E, padx=5, pady=5, columnspan=2)
        
        # Diretório de saída
        ttk.Label(frame, text="Diretório de saída:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        output_entry = ttk.Entry(frame, textvariable=self.output_dir_var, width=50)
        output_entry.grid(row=1, column=1, sticky=tk.W+tk.E, padx=5, pady=5)
        ttk.Button(frame, text="Procurar...", command=self.browse_output_dir).grid(row=1, column=2, padx=5, pady=5)
        
        # Prefixo do site
        ttk.Label(frame, text="Prefixo do site:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        prefix_entry = ttk.Entry(frame, textvariable=self.site_prefix_var, width=50)
        prefix_entry.grid(row=2, column=1, sticky=tk.W+tk.E, padx=5, pady=5, columnspan=2)
        ttk.Label(frame, text="(Nome a ser substituído por 'Raiz' na hierarquia)").grid(row=3, column=1, sticky=tk.W, padx=5)
        
        # Modo de teste
        test_check = ttk.Checkbutton(frame, text="Modo de teste (limitar quantidade de páginas)", variable=self.test_mode_var)
        test_check.grid(row=4, column=0, columnspan=3, sticky=tk.W, padx=5, pady=5)
        
        # Botão de execução
        execute_button = ttk.Button(frame, text="Iniciar Processo Completo", command=lambda: self.prepare_execution("full"))
        execute_button.grid(row=5, column=0, columnspan=3, pady=10)
        
        # Configurar expansão da coluna
        frame.columnconfigure(1, weight=1)
        
    def create_advanced_tab(self):
        """Criar conteúdo da aba de configurações avançadas"""
        frame = ttk.LabelFrame(self.advanced_tab, text="Configurações Avançadas")
        frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Requisições concorrentes
        ttk.Label(frame, text="Requisições concorrentes:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        concurrent_spin = ttk.Spinbox(frame, from_=1, to=30, textvariable=self.concurrent_var, width=5)
        concurrent_spin.grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Label(frame, text="(Quantidade máxima de requisições simultâneas)").grid(row=0, column=2, sticky=tk.W, padx=5, pady=5)
        
        # Taxa de requisições
        ttk.Label(frame, text="Taxa de requisições:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        rate_spin = ttk.Spinbox(frame, from_=1, to=20, textvariable=self.rate_var, width=5)
        rate_spin.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Label(frame, text="(Requisições por segundo)").grid(row=1, column=2, sticky=tk.W, padx=5, pady=5)
        
        # Separador
        ttk.Separator(frame, orient=tk.HORIZONTAL).grid(row=2, column=0, columnspan=3, sticky=tk.E+tk.W, pady=10)
        
        # Limpar logs
        ttk.Button(frame, text="Limpar Logs", command=self.clear_logs).grid(row=3, column=0, padx=5, pady=5)
        
        # Restaurar padrões
        ttk.Button(frame, text="Restaurar Padrões", command=self.restore_defaults).grid(row=3, column=1, padx=5, pady=5)
    
    def create_log_area(self):
        """Criar área de logs"""
        log_frame = ttk.LabelFrame(self.main_frame, text="Logs")
        log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Área de texto com scrollbar
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, height=10)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.log_text.config(state=tk.DISABLED)  # Apenas leitura
        
    def create_status_bar(self):
        """Criar barra de status"""
        self.status_var = tk.StringVar(value="Pronto")
        status_bar = ttk.Label(self, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        
    def process_log_queue(self):
        """Processar a fila de logs e atualizar a interface"""
        try:
            while True:
                record = log_queue.get_nowait()
                self.update_log(record)
                log_queue.task_done()
        except queue.Empty:
            self.after(100, self.process_log_queue)
            
    def update_log(self, record):
        """Atualizar a área de log com uma nova mensagem"""
        msg = log_formatter.format(record)
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, msg + '\n')
        self.log_text.see(tk.END)  # Rolar para o final
        self.log_text.config(state=tk.DISABLED)
        
    def clear_logs(self):
        """Limpar a área de logs"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
        logger.info("Logs limpos")
        
    def browse_csv(self):
        """Abrir diálogo para selecionar arquivo CSV"""
        csv_path = filedialog.askopenfilename(
            title="Selecionar arquivo CSV",
            filetypes=[("Arquivos CSV", "*.csv"), ("Todos os arquivos", "*.*")]
        )
        if csv_path:
            self.csv_file_var.set(csv_path)
            logger.info(f"Arquivo CSV selecionado: {csv_path}")
            
            # Tentar detectar automaticamente o nome do site a partir do nome do arquivo
            filename = os.path.basename(csv_path)
            site_name_match = re.search(r'([a-zA-Z0-9_-]+)_mapeamento\.csv', filename)
            if site_name_match:
                site_name = site_name_match.group(1).replace('_', ' ').title()
                if messagebox.askyesno("Nome do site detectado", 
                                      f"Nome do site detectado: '{site_name}'\nDeseja usar este nome como prefixo?"):
                    self.site_prefix_var.set(site_name)
                    logger.info(f"Prefixo do site definido automaticamente: {site_name}")
            
    def browse_output_dir(self):
        """Abrir diálogo para selecionar diretório de saída"""
        output_dir = filedialog.askdirectory(
            title="Selecionar diretório de saída",
            initialdir=self.output_dir_var.get()
        )
        if output_dir:
            self.output_dir_var.set(output_dir)
            logger.info(f"Diretório de saída selecionado: {output_dir}")
            
    def prepare_execution(self, command):
        """Preparar para executar um comando"""
        if self.running:
            messagebox.showerror("Processo em andamento", "Já existe um processo em execução. Aguarde ou interrompa-o.")
            return
            
        # Validar entradas conforme o comando
        if command in ['map', 'full']:
            if not self.url_var.get().strip():
                messagebox.showerror("Erro", "Por favor, informe a URL do site.")
                return
                
        if command == 'format':
            if not self.csv_file_var.get().strip():
                messagebox.showerror("Erro", "Por favor, selecione um arquivo CSV.")
                return
                
        # Iniciar execução em uma thread separada
        self.running = True
        self.update_status(f"Executando: {command}")
        
        # Construir comando
        cmd_args = self.build_command_args(command)
        
        # Iniciar thread
        thread = threading.Thread(target=self.execute_command, args=(command, cmd_args))
        thread.daemon = True
        thread.start()
        
    def build_command_args(self, command):
        """Construir argumentos para o comando baseado na interface"""
        args = {}
        
        # Argumentos comuns
        args['output_dir'] = self.output_dir_var.get()
        
        # Argumentos específicos por comando
        if command in ['map', 'full']:
            args['url'] = self.url_var.get()
            args['test_mode'] = self.test_mode_var.get()
            args['concurrent'] = self.concurrent_var.get()
            args['rate'] = self.rate_var.get()
            
        if command in ['format', 'full']:
            site_prefix = self.site_prefix_var.get()
            if site_prefix:
                args['site_prefix'] = site_prefix
                
        if command == 'format':
            args['csv_file'] = self.csv_file_var.get()
            
        return args
        
    def execute_command(self, command, args):
        """Executar o comando (em uma thread separada)"""
        logger.info(f"Executando comando: {command}")
        logger.info(f"Argumentos: {args}")
        
        try:
            # Importar funções do main.py no contexto atual
            from main import run_mapping, run_formatting, run_full_process
            
            # Executar conforme o comando
            if command == 'map':
                # Criar loop de eventos para asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                self.raw_csv_path = loop.run_until_complete(run_mapping(
                    url=args['url'],
                    output_dir=args['output_dir'],
                    test_mode=args['test_mode'],
                    concurrent=args['concurrent'],
                    rate=args['rate']
                ))
                
                if self.raw_csv_path:
                    self.show_success_dialog("Mapeamento", self.raw_csv_path)
                else:
                    self.show_error_dialog("Mapeamento")
                    
            elif command == 'format':
                self.raw_csv_path = args['csv_file']
                site_prefix = args.get('site_prefix')
                
                self.raw_csv_path, self.excel_output_path = run_formatting(
                    csv_path=args['csv_file'],
                    output_dir=args['output_dir'],
                    site_prefix=site_prefix
                )
                
                if self.excel_output_path:
                    self.show_success_dialog("Formatação", self.excel_output_path)
                else:
                    self.show_error_dialog("Formatação")
                    
            elif command == 'full':
                # Criar loop de eventos para asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                site_prefix = args.get('site_prefix')
                
                self.raw_csv_path, self.excel_output_path = loop.run_until_complete(run_full_process(
                    url=args['url'],
                    output_dir=args['output_dir'],
                    site_prefix=site_prefix,
                    test_mode=args['test_mode'],
                    concurrent=args['concurrent'],
                    rate=args['rate']
                ))
                
                if self.excel_output_path:
                    self.show_success_dialog("Processo completo", self.excel_output_path)
                else:
                    self.show_error_dialog("Processo completo")
                
        except Exception as e:
            logger.error(f"Erro ao executar comando: {str(e)}", exc_info=True)
            messagebox.showerror("Erro na execução", f"Ocorreu um erro ao executar o comando:\n\n{str(e)}\n\nConsulte os logs para mais detalhes.")
            
        finally:
            self.running = False
            self.update_status("Pronto")
            
    def show_success_dialog(self, operation_type, output_path):
        """Mostrar diálogo de sucesso com opções para abrir o arquivo"""
        result = messagebox.askyesno(
            "Operação concluída",
            f"{operation_type} concluído com sucesso!\n\nArquivo gerado: {output_path}\n\nDeseja abrir o arquivo?",
            icon=messagebox.INFO
        )
        
        if result:
            self.open_result_file(output_path)
            
    def show_error_dialog(self, operation_type):
        """Mostrar diálogo de erro"""
        messagebox.showerror(
            "Operação falhou",
            f"{operation_type} falhou. Verifique os logs para mais detalhes."
        )
        
    def open_result_file(self, file_path):
        """Abrir arquivo de resultado com o aplicativo padrão"""
        if not file_path:
            messagebox.showinfo("Informação", "Nenhum arquivo disponível para abrir.")
            return
            
        if not os.path.exists(file_path):
            messagebox.showerror("Arquivo não encontrado", f"O arquivo {file_path} não foi encontrado.")
            return
            
        logger.info(f"Abrindo arquivo: {file_path}")
        
        try:
            import platform
            if platform.system() == 'Windows':
                os.startfile(file_path)
            elif platform.system() == 'Darwin':  # macOS
                import subprocess
                subprocess.call(('open', file_path))
            else:  # Linux
                import subprocess
                subprocess.call(('xdg-open', file_path))
                
        except Exception as e:
            logger.error(f"Erro ao abrir arquivo: {str(e)}")
            messagebox.showerror("Erro ao abrir", f"Não foi possível abrir o arquivo:\n{str(e)}")
            
    def open_output_dir(self):
        """Abrir o diretório de saída no explorador de arquivos"""
        if not os.path.exists(self.output_dir_var.get()):
            messagebox.showerror("Diretório não encontrado", "O diretório de saída não existe.")
            return
            
        try:
            import platform
            if platform.system() == 'Windows':
                os.startfile(self.output_dir_var.get())
            elif platform.system() == 'Darwin':  # macOS
                import subprocess
                subprocess.call(('open', self.output_dir_var.get()))
            else:  # Linux
                import subprocess
                subprocess.call(('xdg-open', self.output_dir_var.get()))
                
        except Exception as e:
            logger.error(f"Erro ao abrir diretório: {str(e)}")
            messagebox.showerror("Erro ao abrir", f"Não foi possível abrir o diretório:\n{str(e)}")
            
    def stop_process(self):
        """Interromper o processo em execução"""
        if not self.running:
            messagebox.showinfo("Informação", "Não há processo em execução para interromper.")
            return
            
        logger.warning("Solicitação para interromper o processo.")
        # TODO: Implementar mecanismo para interromper processos assíncronos
        messagebox.showinfo("Não implementado", 
                          "A interrupção do processo não está implementada nesta versão.\n\nFeche a aplicação se necessário.")
            
    def update_status(self, message):
        """Atualizar a barra de status"""
        self.status_var.set(message)
        self.update_idletasks()
        
    def set_theme(self, theme_name):
        """Configurar o tema da interface"""
        if theme_name == "default":
            # Restaurar tema padrão do sistema
            self.style.theme_use('default')
            self.log_text.config(bg="white")