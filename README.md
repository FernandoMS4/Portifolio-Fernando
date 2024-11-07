Estou animado para compartilhar um novo projeto que desenvolvi em Python, usando PySpark para tratar dados de uma API de um jogo que marcou minha vida: Ragnarok. 🎮✨
Para aqueles que já jogaram ou ainda jogam Ragnarok e sentem uma grande nostalgia ao lembrar do game, este projeto tem como objetivo coletar dados do jogo e aplicar e aprender técnicas de engenharia e BI.

Objetivo do Projeto ⚒:
Captura de Dados 📊: Utilizei Python para requisitar dados da API e estruturá-los em um DataFrame.
Armazenamento 💾: Inserção do DataFrame em um Data Lake fictício e uso do Render para representação, visando segurança e facilidade de uso.

![image](https://github.com/user-attachments/assets/67593867-5ca1-4e48-854f-ad2bb4789d9c)


Tratamento e Limpeza 🧹: Usei PySpark para corrigir erros, limpar dados incorretos e estruturar novas tabelas para consulta em um BI sem retrabalho.
Exportação 📤: Transferência das novas tabelas para um Data Warehouse fictício usando MySQL.

![image](https://github.com/user-attachments/assets/cd73ef2f-59cc-4d7a-9eea-f937b4ae4a4c)


Visualização 📈: Criação de um BI para visualização dos dados.

![image](https://github.com/user-attachments/assets/9dcddd1c-425a-4ab7-b649-ab72a1f7484e)


Principais Desafios 🏗:
Requisições da API em Lotes 🗂️: Muitos dados e incerteza sobre o range de IDs. Usei a biblioteca "current" para facilitar requisições em grande escala.

![image](https://github.com/user-attachments/assets/05620d34-72bf-48f4-9fd1-a50ea2f3db6d)

Instalação do PySpark 🛠️: Realizei o projeto no Windows, enfrentando desafios na instalação do Spark, que consegui resolver após muitos erros.
Uso do Spark 🔍: Primeiro contato com a ferramenta, aprendendo a utilizar bibliotecas e estruturar tabelas através de pesquisa e prática contínua.


Tecnologias Utilizadas:
Python
PySpark
Render
requests
current.futures
DoteEnv
findspark
Dbeaver
