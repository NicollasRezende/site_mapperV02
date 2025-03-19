Coletando informações do workspace# README

## Descrição

Este projeto é uma ferramenta para mapeamento de sites e formatação de resultados em planilhas Excel. Ele integra duas funcionalidades principais:

1. **SiteMapper** - Realiza o crawling e mapeamento do site.
2. **PlanilhaFormatter** - Formata os resultados para Excel com uma estrutura padronizada.

## Estrutura do Projeto

```
__init__.py
gui.py
main.py
planilha_formatter.py
site_mapper.py
models/
    __init__.py
    page_data.py
services/
    __init__.py
    excel_service.py
    page_node.py
    site_mapper.py
utils/
    __init__.py
    file_utils.py
    url_utils.py
```

## Requisitos

-   Python 3.7 ou superior
-   Bibliotecas listadas em `requirements.txt`

## Instalação

1. Clone o repositório:

    ```sh
    git clone https://github.com/seu-usuario/seu-repositorio.git
    cd seu-repositorio
    ```

2. Crie um ambiente virtual e ative-o:

    ```sh
    python -m venv venv
    source venv/bin/activate  # No Windows use `venv\Scripts\activate`
    ```

3. Instale as dependências:
    ```sh
    pip install -r requirements.txt
    ```

## Uso

### Comandos

-   **Mapeamento de site:**

    ```sh
    python main.py map <url> [--test] [--output DIR] [--concurrent NUM] [--rate NUM]
    ```

    Exemplo:

    ```sh
    python main.py map https://tarf.economia.df.gov.br --output ./resultados
    ```

-   **Formatação de CSV para Excel:**

    ```sh
    python main.py format <csv_file> [--output DIR] [--site_prefix NAME]
    ```

    Exemplo:

    ```sh
    python main.py format mapeamento.csv --site_prefix "Tribunal Administrativo de Recursos Fiscais"
    ```

-   **Processo completo (mapeamento e formatação):**

    ```sh
    python main.py full <url> [--test] [--output DIR] [--site_prefix NAME]
    ```

    Exemplo:

    ```sh
    python main.py full https://tarf.economia.df.gov.br --site_prefix "Tribunal Administrativo de Recursos Fiscais"
    ```

-   **Iniciar a interface gráfica:**
    ```sh
    python main.py gui
    ```

### Argumentos

-   `map <url>`: URL do site a ser mapeado.
-   `format <csv_file>`: Caminho para o arquivo CSV gerado pelo mapeador.
-   `full <url>`: URL do site a ser mapeado e formatado.
-   `gui`: Inicia a interface gráfica.

#### Opções

-   `--test`: Executar em modo de teste (limite de páginas).
-   `--output DIR`: Diretório para salvar resultados (padrão: `output`).
-   `--concurrent NUM`: Número máximo de requisições concorrentes (padrão: 10).
-   `--rate NUM`: Requisições por segundo (padrão: 5).
-   `--site_prefix NAME`: Nome do site a substituir por "Raiz".
-   `--site_name NAME`: Nome do site para incluir no arquivo.

## Exemplo de Uso

1. Mapeamento de um site:

    ```sh
    python main.py map https://tarf.economia.df.gov.br --output ./resultados
    ```

2. Formatação de um arquivo CSV:

    ```sh
    python main.py format mapeamento.csv --site_prefix "Tribunal Administrativo de Recursos Fiscais"
    ```

3. Processo completo de mapeamento e formatação:

    ```sh
    python main.py full https://tarf.economia.df.gov.br --site_prefix "Tribunal Administrativo de Recursos Fiscais"
    ```

4. Iniciar a interface gráfica:
    ```sh
    python main.py gui
    ```

## Logs

Os logs são gerados no arquivo `main.log` e no console. Certifique-se de verificar os logs para detalhes sobre a execução e possíveis erros.
