# Datalake Docker (Airflow, Spark, Jupyter Nootebook, PostgreSQL e PgAdmin)

Esse é um projeto desenvolvido com o intuito de ser um ambiente datalake completo de fácil execução em containers docker. Atualmente o projeto conta com os seguintes recursos:

- Airflow 1.10.7
- Spark 3.1.2 (bitnami)
- Postgresql 9.6
- PgAdmin 4
- Jupyter Notebook (Pyspark Spark-3.1.2)

## Sobre o ambiente
O ambiente do projeto é construido usando 5 imagens docker.
- Airflow (Build sobre a imagem python:3.6-stretch)
- bitnami/spark:3.1.2
- postgres:9.6
- dpage/pgadmin4
- jupyter/pyspark-notebook:spark-3.1.2
![](https://raw.githubusercontent.com/LucasRangelSSouza/DatalakeDocker/main/doc/diagramaImagens.png)

As imagens são executadas em 7 containers dentro do docker, sendo que 3 containers são referentes a 3 workers spark trabalhando paralelamente.
![](https://raw.githubusercontent.com/LucasRangelSSouza/DatalakeDocker/main/doc/diagramaContaines.png)

Sendo assim a arquitetura do ambiente é orquestrada de acordo com o diagrama abaixo.
![](https://raw.githubusercontent.com/LucasRangelSSouza/DatalakeDocker/main/doc/diagramaServicos.png)

## Subindo o ambiente

O ambiente é executado em containers docker, sendo assim é necessário que a maquina o qual vai rodar o ambiente tenha o docker instalado.
###### 1º Clone o repositorio DatalakeDocker
* Abra o o terminal ou o cmd e Navegue até um diretório pré-existente de sua escolha:
    ```
    C:\> chdir C:\<MeuDiretorio>\
    ```
* Clone o repositorio DatalakeDocker
    ```
    C:\> git clone https://github.com/LucasRangelSSouza/DatalakeDocker
    ``

###### 2º Execute o docker compose
* Abra o o terminal de comandos do seu SO e Navegue até a pasta docker dentro do diretorio onde o repositorio foi clonado:
    ```
    C:\> chdir C:\<MeuDiretorio>\DatalakeDocker\docker
    ```
* Realize o build e inicie os containers com o comando. (A primeira execução pode demorar mais pois o docker ira realizar o download dos artefatos utilizados nos projetos):
    ```
    C:\> docker compose up
    ```
* Em um outro terminal ou cmd verifique se os containers foram iniciados com sucesso utilizando o comando:
    ```
    C:\> docker ps -a
    ```
 * Caso todo processo tenha ocorrido normalmente você vera como a seguir:
 ![](https://raw.githubusercontent.com/LucasRangelSSouza/DatalakeDocker/main/doc/composeup.png)
 ###### 4º Teste o acesso as ferramentas:
- Airflow : http://localhost:8282/admin/
- Spark Master: http://localhost:8181/
- PgAdmin 4: http://localhost:16543/
- Jupyter Notebook: http://127.0.0.1:8888/

Se você conseguir visualizar a tela inicial de todos os endereços todo o ambiente foi construído com sucesso, e está pronto para ser configurado.
 
## Configurando o ambiente
###### 1º Configurando o airflow  e o spark:
* Para configurar o airflow e o spark, abra a interface do airflow http://localhost:8282/admin/. ![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/airflowHome.png)

* Na barra de pesquisa no canto superior direito digite **Prepare_lakehouse** (como na imagem abaixo) e pressione enter. Sera exibida a dag Prepare_lakehouse, clique sobre ela para abrir a dag.
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/airflowPesquisa.png)

* Sera exibida a visualização da dag em arvore como na imagem abaixo, clique em **Graph View** e logo apos em **Trigger Dag** no canto superior direito.
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/airflowGrap.png)

* Será exibida um alerta como na imagem abaixo indagando se deseja mesmo iniciar a dag, clique em sim.
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/airflowTriggerDag.png)

* Neste momento a dag está sendo executada e as configuraçoes sendo realizadas, aquarde alguns minutos, e va pressionando de vez enquanto o botão reload para ver o progresso da execução.
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/airflowReload.png)

* Assim que todos os blocos da dag estiverem contornados de verde escuro, a configuração terminou, e o ambiente Airflow e Spark estão configurados e prontos para uso.
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/airflowPrepareOK.png)

###### 2º Acessando  e autenticando no Jupyter notebook:
* Para autenticar no jupyter notebook, é necessario um token gerado aleatoreamente pelo jupyter a cada execução. Para obter esse token, abra um terminal ou cmd e digite o seguinte comando:
    ```
    C:\> docker logs -f docker-jupyter-spark-1 
    ```
* Você vera logs semelhantes a imagem abaixo, role até a parte final dos logs, e você vera o token de acesso como marcado em vermelho na imagem abaixo. O token é apenas o hash, no caso da imagem abaixo o token seria:  26db7d445966c69486dc8bd0dd864046502fa1639088219c
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/jupyterToken.png)

* Em posse do numero do token navegue até a url da home do jupyter: http://127.0.0.1:8888/ . Você vera a interface como na imagem abaixo. Cole o token obtido anteriormente na caixa de texto **Password or token**, e depois clique em login. 
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/jupyterLogin.png)

* Se você consegue ver a interface como na imagem abaixo o Jupyter Notebook está pronto para ser utilizado. (No diretório -  work/notebook/ existem alguns notebooks com exemplos que podem ser uteis.) 
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/jupyterOK.png)

###### 3º Acessando o PgAdmin  e autenticando no servidor Postgresql :
* Para acessar a interface do PgAdmin  acesse a URL: http://localhost:16543/ . Será exibida a tela de login do PgAdmin 4 como na imagem abaixo. 
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/PgAdminLogin.png)
 * Os dados padrão de login do PgAdmin são:
 ***Login**: user@pgadmin.com
 ***Senha**: PgAdmin@c4680
 
* Após realizar o login na interface do pgAdmin, você ja pode registrar o servidor postgresql executando no container docker utilizando as seguintes credenciais: (Observe que essas credenciais são validas apenas para aplicativos que executam dentro do docker)
 ***Hostname**: postgres
 ***Port**: 5432
 ***Manteince Database**: test
 ***Username**: test
 ***Password**: postgres 


## Testando o ambiente (pipeline-microdados)

A fim de testar o ambiente, foi desenvolvido um pipeline de dados que realiza o download de dados do enem 2020 e realiza a transformação e tratativa dos dados, sendo assim nomeado de **pipeline-microdados**.
### Sobre o pipeline-microdados.
O pipeline realiza o download de arquivos do site do inep, e realiza o tratamento e a modelagem dos dados em star schema, os dados finais são dispostos em camadas dentro do lake (montado dentro da folder lake do repositorio), bem como os dados da camada business são salvos na base de dados relacional postgress. Observe abaixo o desenho de solução na arquitetura dos serviços do lakehouse docker. 
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/diagramaDesenhoSolucao.png)

Observe a modelagem star schema disponibilizada na business e na base de dados postgresql.
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/diagramaStarSchema.png)

Confira os códigos das transformações realizadas no pipeline: 
[Dag Airflow / DownloadFile](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/)
[landing2raw](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/)
[raw2rusted](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/)
[fato_enem](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/)
[dim_sexo](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/)
[dim_raca](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/)
[dim_escola](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/)
[dim_ensino](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/)
[dim_uf](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/)
[dim_municipio](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/)
[dim_zona](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/)
[dim_situacao_escola](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/)
[save_postgress](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/)
### Executando o pipeline-microdados.
* Para executar o pipeline-microdados, abra a interface do airflow http://localhost:8282/admin/. ![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/airflowHome.png)

* Na barra de pesquisa no canto superior direito digite **pipeline-microdados** (como na imagem abaixo) e pressione enter. Sera exibida a dag pipeline-microdados, clique sobre ela para abrir a dag.
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/airflowMicrodadosA.png.png)

* Sera exibida a visualização da dag em arvore como na imagem abaixo, clique em **Graph View** e logo apos em **Trigger Dag** no canto superior direito.
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/airflowMicrodadosB.png)

* Será exibida um alerta como na imagem abaixo indagando se deseja mesmo iniciar a dag, clique em sim.
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/airflowTriggerDag.png)

* Neste momento a dag está sendo executada e as etapas sendo realizadas, aquarde alguns minutos (geralmente demora entre 15 a 20 minutos), e vá pressionando de vez enquanto o botão reload para ver o progresso da execução.
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/airflowMicrodadosC.png)

* Assim que todos os blocos da dag estiverem contornados de verde escuro, a execução terminou e os dados já estão disponíveis nas folders do lake e na base de dados postgreSQL.
![](https://github.com/LucasRangelSSouza/criptoCurrency/blob/main/doc/airflowMicrodadosD.png)
### Validando os resultados pipeline-microdados.


## Contribuições e to do
 Temos algumas melhorias em vista para este projeto, sinta-se a vontade para contribuir.
- Upgrade airflow para a versão 2.0 ou superior.
- Upgrade postgresql para a versão 12 ou superior.
- Upgrade kernel jupyter para suportar R alem de pyspark.
- Adicionar uma ferramenta de dataviz open source
## Licença
MIT
Todas as ferramentas utilizadas aqui são open source, e todo trabalho realizado nesse projeto também é livre para ser utilizado para qualquer fim. 
**Free Software, Hell Yeah!**