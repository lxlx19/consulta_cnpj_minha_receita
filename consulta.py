import re

import pandas as pd
import requests as req


class CNPJConsulta:
    """
    Classe para consultar informações de CNPJs a partir de um arquivo CSV e
    processar os resultados.
    Atributos:
        URI (str): URL base para consulta de CNPJs.
        csv_file (str): Caminho para o arquivo CSV contendo os CNPJs.
        cnpjs (list): Lista de CNPJs extraídos do arquivo CSV.
        result_df (pd.DataFrame): DataFrame para armazenar os resultados
        das consultas.
    Métodos:
        __init__(csv_file):
            Inicializa a classe com o caminho do arquivo CSV.
        _ler_csv():
            Lê o arquivo CSV e retorna uma lista de CNPJs.
        consulta_cnpj(cnpj):
            Consulta informações de um CNPJ específico.
        processar_consultas():
            Processa as consultas para todos os CNPJs no arquivo CSV.
        normalizar_dados():
            Normaliza os dados resultantes das consultas.
        salvar_resultado(output_file):
            Salva os resultados normalizados em um arquivo CSV.
        executar(output_file):
            Executa o processo completo de consulta, normalização e
            salvamento dos resultados.
    """

    URI = "https://minhareceita.org/"

    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.cnpjs = self._ler_csv()
        self.result_df = None

    def _ler_csv(self):
        df = pd.read_csv(self.csv_file)
        return df["cnpj"].tolist()

    def consulta_cnpj(self, cnpj):
        """
        Consulta as informações do CNPJ (Cadastro Nacional da Pessoa Jurídica)
        a partir de uma URL especificada.

        Args:
            cnpj (str): O número do CNPJ a ser consultado.

        Returns:
            dict: A resposta JSON da requisição contendo as informações do
            CNPJ.
        """
        url = self.URI + cnpj
        response = req.get(url)
        return response.json()

    def processar_consultas(self):
        """
        Processa uma lista de números de CNPJ (Cadastro Nacional da
        Pessoa Jurídica) realizando uma consulta de CNPJ para cada número,
        normalizando os dados resultantes e concatenando-os em um
        único DataFrame.

        Este método itera sobre a lista de números de CNPJ armazenados no
        atributo `self.cnpjs`, realiza uma consulta de CNPJ para cada
        número usando o método `consulta_cnpj`, normaliza os dados
        JSON resultantes em uma tabela plana e adiciona ao DataFrame
        `self.result_df`.

        Se `self.result_df` for None, ele inicializa com o primeiro
        DataFrame. Caso contrário, ele concatena o novo DataFrame
        com o DataFrame `self.result_df` existente.

        Atributos:
            self.cnpjs (list): Uma lista de números de CNPJ a serem
            processados.
            self.result_df (pd.DataFrame ou None): Um DataFrame para
            armazenar os resultados concatenados das consultas de CNPJ.

        Retorna:
            None
        """
        for cnpj in self.cnpjs:
            data = self.consulta_cnpj(cnpj)
            normalized_data = pd.json_normalize(data)
            df = pd.DataFrame(normalized_data)
            if self.result_df is None:
                self.result_df = df
            else:
                self.result_df = pd.concat(
                    [self.result_df, df], ignore_index=True
                    )

    def normalizar_dados(self):
        """
        Normaliza os dados no DataFrame de resultados realizando as seguintes
        operações:

        1. Remove a coluna "qsa".
        2. Expande a coluna "cnaes_secundarios" em colunas separadas usando
        a normalização JSON.
        3. Remove a coluna original "cnaes_secundarios".
        4. Renomeia colunas que começam com um dígito para
        "cnae_secundario_<nome_original_da_coluna>".

        Retorna:
            None: O método modifica o atributo result_df no local.
        """
        self.result_df = self.result_df.drop(columns=["qsa"])
        self.result_df = pd.concat(
            [self.result_df, pd.json_normalize(
                self.result_df["cnaes_secundarios"]
                )],
            axis=1,
        )
        self.result_df = self.result_df.drop(columns=["cnaes_secundarios"])
        columns = self.result_df.columns
        for column in columns:
            if re.match(r"^\d+", str(column)):
                self.result_df = self.result_df.rename(
                    columns={column: "cnae_secundario_" + str(column)}
                )

    def salvar_resultado(self, output_file):
        """
        Saves the result DataFrame to a CSV file.

        Parameters:
        output_file (str): The path to the output CSV file.

        Returns:
        None
        """
        self.result_df.to_csv(output_file, index=False)

    def executar(self, output_file):
        """
        Executa o processo completo de consulta, normalização e salvamento
        dos dados.

        Args:
            output_file (str): O caminho do arquivo onde o resultado será
            salvo.

        Returns:
            None
        """
        self.processar_consultas()
        self.normalizar_dados()
        self.salvar_resultado(output_file)


if __name__ == "__main__":
    consulta = CNPJConsulta("cnpj.csv")
    consulta.executar("resultado.csv")
