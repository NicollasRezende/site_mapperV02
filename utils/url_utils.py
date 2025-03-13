import unicodedata
from urllib.parse import urlparse, urljoin

class URLUtils:
    def __init__(self, domain: str):
        self.domain = domain
        self.news_categories = {
            "noticias", 
            "destaques principais", 
            "destaques secretaria", 
            "destaques sem foto", 
            "destaque", 
            "todas as notícias", 
            "destaques principais carrossel",
            "noticias da secretaria",
            "módulo destaques da secretaria",
            "módulo carrossel de destaques principais",
            "módulo destaques sem foto - fundo azul",
            "categoria",
            "a secretária",
            "módulo destaques do tarf",
            "modulo-15-botoes",
            "modulo carrossel de destaques principais",
            "sala de imprensa",
            "Secretaria na mídia",
            "Notícias do TARF",
            "Módulo Destaques com fotos - FUNDO AZUL"
            "Módulo Destaques sem foto-FUNDO AZUL",
            "carrossel de destaques",
            "modulo Destaques com fotos - fundo azul"
        }


    @staticmethod
    def remove_accents(text: str) -> str:
        """Remove acentos de uma string."""
        return ''.join(
            char for char in unicodedata.normalize('NFKD', text)
            if not unicodedata.combining(char)
        )

    def is_news_breadcrumb(self, breadcrumb: list[str]) -> bool:
        """
        Verifica se algum item do breadcrumb está na lista de categorias de notícias.
        Ignora acentos e maiúsculas/minúsculas.
        """
        if not breadcrumb:
            return False
        
        # Normalize news categories (remove accents, lowercase)
        normalized_categories = {
            self.remove_accents(cat.lower().strip()) for cat in self.news_categories
        }
        
        return any(
            self.remove_accents(item.lower().strip()) in normalized_categories
            for item in breadcrumb
        )

    def is_internal_file(self, url: str) -> bool:
        parsed = urlparse(url)
        path = parsed.path.lower()
        
        file_extensions = [
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', 
            '.jpg', '.jpeg', '.png', '.gif', '.mp4', '.mp3', 
            '.wav', '.pptx', '.txt', '.html'
        ]
        
        file_patterns = ['/documents/', '/wp-content/', '/wp-conteudo/']
        
        is_file = any(ext in path for ext in file_extensions)
        is_wp_file = any(pattern in path for pattern in file_patterns)
        
        return (
            (self.domain in parsed.netloc or not parsed.netloc) and
            (is_file or is_wp_file)
        )

    def is_valid_internal_url(self, url: str) -> bool:
        if not url:
            return False
            
        parsed = urlparse(url)
        
        ignore_patterns = [
            '.jpg', '.jpeg', '.png', '.gif', '.pdf', '.doc', '.docx',
            '.xls', '.xlsx', '.zip', '.rar', '.mp3', '.mp4',
            'wp-content', 'wp-includes', 'wp-json',
            'feed', 'comments', 'trackback', 'attachment'
        ]
        
        return (
            parsed.scheme in ['http', 'https'] and
            (self.domain in parsed.netloc or not parsed.netloc) and
            not any(pattern in url.lower() for pattern in ignore_patterns) and
            '#' not in url and
            '?' not in url
        )

    def is_valid_url(self, url: str) -> bool:
        if not url:
            return False
                
        parsed = urlparse(url)
        return (
            parsed.scheme in ['http', 'https'] and
            (self.domain in parsed.netloc or not parsed.netloc) and
            not any(ext in url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.pdf', '.doc', '.docx']) and
            '#' not in url and
            'wp-content' not in url.lower() and
            'wp-includes' not in url.lower()
        )

    def is_external_gov_link(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            gdf_domains = ['.df.gov.br']
            
            is_gdf = any(domain in parsed.netloc.lower() for domain in gdf_domains)
            is_different_domain = self.domain.lower() != parsed.netloc.lower()
            
            return is_gdf and is_different_domain
        except:
            return False

    def is_news_url(self, url: str) -> bool:
        parsed = urlparse(url)
        path = parsed.path.lower()
        
        news_indicators = [
            '/category/noticias',
            '/noticias/',
            '/todas-as-noticias',
            '/category/servicos-ao-cidadao',
            '/category/modulo-destaques'
        ]
        
        return any(indicator in path for indicator in news_indicators)