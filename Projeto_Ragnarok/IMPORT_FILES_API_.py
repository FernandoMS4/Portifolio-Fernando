import concurrent.futures
import requests
import pandas as pd

def fetch_monster_data(monster_id):
    url = f"https://ragnapi.com/api/v1/re-newal/monsters/{monster_id}"
    response = requests.get(url)
    if response.status_code == 200:
        try:
            return response.json()
        except ValueError:
            print('CatchErr')
    else:
        print('Code: ERROR')
        return None

# Parâmetros para a função
start_id = 1000
end_id = 5000
chunk_size = 1

# Executar as requisições em paralelo
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    monster_ids = range(start_id, end_id)
    full_file = list(executor.map(fetch_monster_data, monster_ids))

# Filtrar None dos resultados
full_file = [data for data in full_file if data is not None]

# Criar DataFrame a partir dos dados coletados
df = pd.DataFrame(full_file)
df.to_csv('renew_monster_data',index=False,header=True)
