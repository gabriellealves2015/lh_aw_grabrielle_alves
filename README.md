# Projeto de Transformação de Dados - Adventure Works - LH

O objetivo deste projeto é transformar dados para responder perguntas estratégicas da empresa Adventure Works, uma indústria de bicicletas em franco crescimento, que possui mais de 500 produtos distintos e 20 mil clientes.

O projeto foi desenvolvido utilizando o dbt core e transforma dados originados das seguintes fontes do cliente:
* SAP (ERP)
* SalesForce (CRM)
* Google Analytics (Web Analytics)
* Wordpress (Site)

Os modelos foram organizados em quatro estágios:

* Staging (materializado somente em desenvolvimento)
* Intermediate (materializado somente em desenvolvimento)
* Marts
* Aggregated

Foi solicitado a entrega de uma tabela agregada integrando os dados sobre Vendas com base na região e vendedor., esta foi separada na camada "Aggregated".

Os dados prontos para consumo no BI estão direcionados na camada "Marts", e serão entregues via Google Big Query.


## Instruções de utilização

Clone o repositório localmente, crie uma cópia do arquivo ".env.example" e renomeie para ".env".

Ao abrir o arquivo ".env" existirão configurações para desenvolvimento e produção.

- A variável `DEFAULT_TARGET` aceita os valores `dev` (ambiente de desenvolvimento) ou `prod` (ambiente de produção), utilizando as variáveis correspondentes de cada ambiente.

- Na variável `PROD_ | DEV_DBT_PROJECT` adicione o ID do projeto do Google Big Query.
- Na variável `PROD_ | DEV_DBT_DATASET` adicione o nome do seu dataset.
- Na variável `PROD_ | DEV_DBT_KEYFILE` adicione a referência ao path da sua chave json de autenticação ([Link com instruções de criação](https://cloud.google.com/iam/docs/keys-create-delete?hl=pt-br#iam-service-account-keys-create-console)).

Feito isso, crie uma venv em python no seu terminal com python e git habilitado para instalação das dependências do dbt:

* `python -m venv venv`

Habilite a venv criada:

* Windows: `source venv\Script\activate`
* Linux: `source venv\bin\activate`

Carregue o arquivo `.env`:

* `source .env`

Agora siga com a instalação do dbt e das bibliotecas listada no arquivo de requirements e as dependências do dbt:
* `pip install -r requirements.txt`
* `dbt deps`

Neste ponto o dbt já deve estar instalado e configurado.

## Execução do projeto no dbt

Inicialmente teste a conexão com o Big Query com o comando:
* `dbt debug`

#### Carregamento dos dados

Todas as tabelas do banco fonte do SAP da Adventure Works serão carregadas como seeds pelo dbt. Os arquivos .csv com os dados já estão na pasta de seeds.

Para fazer o carregamento de todas as tabelas usem o comando:
- `dbt seed`

Para carregar uma tabela especifíca utilizem o comando:
- `dbt seed -s nome_do_csv`

#### Execução das transformações

Para executar toda a carga de transformações execute na sequência:
* Executará todos os modelos e suas dependência: `dbt run`
* Executará todos os testes de dados: `dbt test`

Assim os dados serão materializados nas camadas em questão, caso tenha problemas e os schemas não tenham sido previamente criados, realize a criação dos schemas de: `staging`, `intermediate`, `marts`, `aggregated` no Google Big Query.

Para rodar os modelos e os testes individualmente execute:
* Modelos individualmente: `dbt run -m nome_do_modelo`
* Testes individualmente: `dbt test -m nome_do_modelo`



### Problemas comuns

Em caso a linha de comando do dbt fique com o status de estar sempre carregando, ou, o job do comando `dbt seed` fique rodando indefinitivamente mesmo após as 64 tabelas forem carregadas você precisará reiniciar o terminal. Para isso, clique nos três pontos no canto inferior direito ou no lado direito da linha de comando e escolha a opção `Restart IDE`.


## Recursos:
- Saiba mais sobre o dbt [na documentação](https://docs.getdbt.com/docs/introduction).
- Confira o [Discourse](https://discourse.getdbt.com/) para perguntas e respostas frequentes.
- Junte-se à [comunidade dbt](http://community.getbdt.com/) para aprender com outros engenheiros de análise.
- Encontre [eventos dbt](https://events.getdbt.com) perto de você.
- Confira [o blog](https://blog.getdbt.com/) para as últimas notícias sobre o desenvolvimento do dbt e melhores práticas.
