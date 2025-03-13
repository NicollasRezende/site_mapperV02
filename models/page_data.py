from datetime import datetime
from typing import List, Set, Optional
from urllib.parse import urlparse
import locale
from dataclasses import dataclass, field

sep: str = ";" if locale.getdefaultlocale()[0] == "pt_BR" else ","

@dataclass
class PageData:
    """Representa os dados de uma página mapeada com a nova estrutura."""
    url: str
    hierarchy: List[str]
    is_visible: bool
    # Campos básicos
    url_destino: str = ""  # Para
    tipo_migracao: str = "Manual"
    qtd_conteudos: int = 0
    qtd_arquivos: int = 0
    verificar_copias: str = f'=SE(CONT.SE(A:A{sep} A3) > 1{sep} "Duplicado"{sep} "Único")'
    menu_lateral: str = "-"
    breadcrumb_hierarchy: List[str] = field(default_factory=list)
    vocabulario: str = ""
    categoria: str = "-"
    redes_sociais: str = "-"
    pontos_atencao: str = "-"
    tipo_pagina: str = "-"
    pagina_vincular: str = "-"
    link_redirecionamento: str = "-"
    complexidade: str = "-"
    layout: str = "-"
    
    # Campos auxiliares
    arquivos_internos: Set[str] = field(default_factory=set)
    arquivos_externos: Set[str] = field(default_factory=set)
    discovered_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def _process_hierarchy(self, hierarchy_list: List[str]) -> List[str]:
        """
        Processa a hierarquia conforme as regras de negócio.
        
        Args:
            hierarchy_list: Lista com a hierarquia a ser processada
            
        Returns:
            Lista processada com a hierarquia final
        """
        if not hierarchy_list:
            return ["Raiz"]
            
        processed = ["Raiz"]  # Sempre começa com Raiz
        
        # Identifica se há um nome de site (segundo nível) que se repete em vários caminhos
        # Se este for repetido em vários níveis, vamos removê-lo dos níveis subsequentes
        if len(hierarchy_list) > 1:
            site_name = hierarchy_list[1] if len(hierarchy_list) > 1 else None
            
            # Adiciona o nome do site apenas uma vez, se existir
            if site_name and site_name not in ["HOME", "Raiz"]:
                processed.append(site_name)
                
                # Adiciona os demais níveis, removendo repetições do nome do site
                for part in hierarchy_list[2:]:
                    # Evita adicionar o nome do site novamente ou entradas vazias/None
                    if part and part != site_name:
                        processed.append(part)
            else:
                # Se não tiver um nome de site identificável, apenas adiciona as partes restantes
                for part in hierarchy_list[1:]:
                    if part and part not in ["HOME", "Raiz"]:
                        processed.append(part)
        
        return processed

    def _update_page_type_and_visibility(self, hierarchy: List[str]):
        """
        Atualiza o tipo_pagina e is_visible baseado no nível hierárquico.
        
        Args:
            hierarchy: Lista com a hierarquia processada
        """
        # Páginas de nível 2 na hierarquia são consideradas páginas principais (visíveis)
        # Por exemplo: ["Raiz", "INSTITUCIONAL"]
        if len(hierarchy) == 2:
            self.tipo_pagina = "Página Definida"
            self.is_visible = True
        # Se tiver mais níveis, são subpáginas (normalmente ocultas)
        else:
            self.tipo_pagina = "Página de Widget"
            self.is_visible = False

    def to_planilha_row(self) -> List[str]:
        """Converte os dados para uma linha da planilha com a ordem atualizada.
           Padrão:
           
           [Links]: 'De', 'Para'
           [Fase de mapeamento]: 'Tipo de migração', 'Qtd de conteúdos', 'Qtd de arquivos', 'Verificar Cópias'
           [Informações sobre a página]: 'Hierarquia', 'Visibilidade', 'Menu Lateral', 'Breadcrumb', 'Vocabulário',
           'Categoria', 'Pontos de atenção', 'Redes sociais', 'Tipo de página', '(Para página de vincular a uma página deste site) Nome da página',
           'Link da página para a qual redireciona', 'Complexidade', 'Layout'
        """
        # IMPORTANTE: Prioriza o breadcrumb_hierarchy se existir, caso contrário usa a hierarchy
        hierarchy_to_use = self.breadcrumb_hierarchy if self.breadcrumb_hierarchy else self.hierarchy
        
        # Processa a hierarquia para exibição
        hierarchy_display = self._process_hierarchy(hierarchy_to_use)
        
        # Atualiza o tipo de página e visibilidade com base na hierarquia processada
        self._update_page_type_and_visibility(hierarchy_display)
        
        # [Links]
        links = [
            self.url,                                   # De
            self.url_destino,                          # Para
        ]
        
        # [Fase de mapeamento]
        fase_mapeamento = [
            self.tipo_migracao,                        # Tipo de migração
            str(self.qtd_conteudos),                   # Qtd de conteúdos 
            str(self.qtd_arquivos),                    # Qtd de arquivos
            self.verificar_copias,                     # Verificar Cópias
        ]
        
        # Para o breadcrumb, mantenha o formato original (sem processamento)
        breadcrumb_display = " > ".join(self.breadcrumb_hierarchy) if self.breadcrumb_hierarchy else "-"
        
        # [Informações sobre a página]
        informacoes_pagina = [
            " > ".join(hierarchy_display),             # Hierarquia (processada)
            "Menu" if self.is_visible else "Oculta",   # Visibilidade
            self.menu_lateral,                         # Menu Lateral
            breadcrumb_display,                        # Breadcrumb (original)
            self.vocabulario,                          # Vocabulário
            self.categoria,                            # Categoria
            self.pontos_atencao,                       # Pontos de atenção 
            self.redes_sociais,                        # Redes sociais
            self.tipo_pagina,                          # Tipo de página
            self.pagina_vincular,                      # (Para página de vincular a uma página deste site) Nome da página 
            self.link_redirecionamento,                # Link da página para a qual redireciona
            self.complexidade,                         # Complexidade
            self.layout,                               # Layout
        ]
        
        # Juntar todas as colunas na ordem correta
        return links + fase_mapeamento + informacoes_pagina