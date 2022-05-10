if [[ $1 =~ [''] ]]; then
  urlRepoKey=$1
  repoKey=$2
  host=$3
else
  urlRepoKey="https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-states.csv"
  repoKey="p971074907"
#  host="ncovid.natalnet.br/datamanager"
  host="localhost:8082"
fi

echo "" >> ./update-repo.log

echo "******************** " > ./update-repo.log
echo "resultado da atualização em: $(date)" >> ./update-repo.log
echo "urlRepoKey: $urlRepoKey" >> ./update-repo.log
echo "repoKey: $repoKey" >> ./update-repo.log

echo "" >> ./update-repo.log
echo "erro na atualização da média móvel no repositório:" >> ./update-repo.log
echo "repositório $repoKey: $(curl -X GET $host/repo/)" >> ./update-repo.log

echo "unidade brl: $(curl -X POST -H 'Content-Type: application/json' -d '{"locale":"brl","columns":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25],"separator":",","connective":"and","cValueIndex":[3,4],"cValue":["TOTAL","TOTAL"]}' $host/repo/$repoKey/slice)" >> ./update-repo.log

echo "" >> ./update-repo.log
echo "erros na execução do particinamento do repositório atualizado: " >> ./update-repo.log
unidades=("ac" "al" "am" "ap" "ba" "ce" "df" "es" "go" "ma" "mt" "ms" "mg" "pa" "pb" "pr" "pe" "pi" "rj" "rn" "ro" "rs" "rr" "sc" "se" "sp" "to")
for unidade in ${unidades[@]}; do
  resultado=$(curl -X POST -H 'Content-Type: application/json' -d '{"locale":"brl:'"${unidade^h}"'","columns":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25],"separator":",","connective":"and","cValueIndex":[3,4],"cValue":["'"${unidade^^}"'","TOTAL"]}' $host/repo/$repoKey/slice)
  echo "unidade $unidade: $resultado" >> ./update-repo.log
done

for unidade in ${unidades[@]}; do
  resultado=$(curl -X GET -H 'Content-Type: application/json' $host/repo/$repoKey/path/brl:"${unidade^h}"/features/date:newDeaths:newCases/window-size/7/as-json/force-save)
  echo "unidade $unidade: $resultado (mavg:7)" >> ./update-repo.log
  resultado=$(curl -X GET -H 'Content-Type: application/json' $host/repo/$repoKey/path/brl:"${unidade^h}"/features/date:newDeaths:newCases/window-size/14/as-json/force-save)
  echo "unidade $unidade: $resultado (mavg:14)" >> ./update-repo.log
  resultado=$(curl -X GET -H 'Content-Type: application/json' $host/repo/$repoKey/path/brl:"${unidade^h}"/features/date:newDeaths:newCases/window-size/28/as-json/force-save)
  echo "unidade $unidade: $resultado (mavg:28)" >> ./update-repo.log
done

