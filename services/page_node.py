
from dataclasses import dataclass, field
from typing import Dict, List

from requests_cache import Optional

from models.page_data import PageData


@dataclass
class PageNode:
    """Representa um nó na árvore de páginas."""
    title: str
    url: Optional[str] = None
    parent: Optional['PageNode'] = None
    children: Dict[str, 'PageNode'] = field(default_factory=dict)
    page_data: Optional[PageData] = None
    sequence_number: int = 0

class PageTree:
    """Gerencia a estrutura hierárquica das páginas."""
    def __init__(self, root_name: str = "Raiz"):
        self.root = PageNode(root_name)
        self.url_to_node: Dict[str, PageNode] = {}
        self.next_sequence = 1

    def add_menu_page(self, hierarchy: List[str], url: str, page_data: PageData) -> PageNode:
        """Adiciona uma página do menu na árvore."""
        current = self.root
        
        for title in hierarchy[1:]:  # Pula o nome do site (raiz)
            if title not in current.children:
                node = PageNode(title=title, parent=current)
                current.children[title] = node
            current = current.children[title]
        
        current.url = url
        current.page_data = page_data
        current.sequence_number = self.next_sequence
        self.next_sequence += 1
        self.url_to_node[url] = current
        return current

    def add_content_page(self, url: str, page_data: PageData, breadcrumb: List[str] = None) -> PageNode:
        """Adiciona uma página de conteúdo na árvore."""
        if not breadcrumb:
            breadcrumb = page_data.hierarchy
            
        # Encontra o melhor pai baseado no breadcrumb
        parent = self.root
        if breadcrumb and len(breadcrumb) > 1:
            for crumb in breadcrumb[1:-1]:  # Skip 'Raiz' e último item
                if crumb in parent.children:
                    parent = parent.children[crumb]
        
        # Cria o nó para a página
        title = breadcrumb[-1] if breadcrumb else page_data.hierarchy[-1]
        node = PageNode(title=title, url=url, parent=parent)
        node.page_data = page_data
        node.sequence_number = self.next_sequence
        self.next_sequence += 1
        
        # Adiciona ao pai na posição correta
        base_title = title
        counter = 1
        while title in parent.children:
            title = f"{base_title} ({counter})"
            counter += 1
        
        parent.children[title] = node
        self.url_to_node[url] = node
        return node

    def update_hierarchies(self):
        """Atualiza todas as hierarquias baseado na estrutura atual."""
        def get_hierarchy(node: PageNode) -> List[str]:
            hierarchy = []
            current = node
            while current:
                hierarchy.insert(0, current.title)
                current = current.parent
            return hierarchy

        for url, node in self.url_to_node.items():
            if node.page_data:
                node.page_data.hierarchy = get_hierarchy(node)