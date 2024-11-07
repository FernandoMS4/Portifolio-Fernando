Estou animado para compartilhar um novo projeto que desenvolvi em Python, usando PySpark para tratar dados de uma API de um jogo que marcou minha vida: Ragnarok. 🎮✨
Para aqueles que já jogaram ou ainda jogam Ragnarok e sentem uma grande nostalgia ao lembrar do game, este projeto tem como objetivo coletar dados do jogo e aplicar e aprender técnicas de engenharia e BI.

Objetivo do Projeto ⚒:
Captura de Dados 📊: Utilizei Python para requisitar dados da API e estruturá-los em um DataFrame.
Armazenamento 💾: Inserção do DataFrame em um Data Lake fictício e uso do Render para representação, visando segurança e facilidade de uso.

![image](https://github.com/user-attachments/assets/67593867-5ca1-4e48-854f-ad2bb4789d9c)


Tratamento e Limpeza 🧹: Usei PySpark para corrigir erros, limpar dados incorretos e estruturar novas tabelas para consulta em um BI sem retrabalho.

![image](https://github.com/user-attachments/assets/190f835d-42e5-48c0-bc7b-b8a6e75e8e9b)

Exportação 📤: Transferência das novas tabelas para um Data Warehouse fictício usando MySQL.

![image](https://github.com/user-attachments/assets/b9b800e9-5e46-42f0-8b25-b29354d90f7a)


Visualização 📈: Criação de um BI para visualização dos dados.

![image](https://github.com/user-attachments/assets/9dcddd1c-425a-4ab7-b649-ab72a1f7484e)


Principais Desafios 🏗:
Requisições da API em Lotes 🗂️: Muitos dados e incerteza sobre o range de IDs. Usei a biblioteca "current" para facilitar requisições em grande escala.

![image](https://github.com/user-attachments/assets/f3bce3f2-9561-441e-8c29-07a5059cc504)


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
Figma

