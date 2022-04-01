if [[ $1 =~ [''] ]]; then
  urlRepoKey=$1
  repoKey=$2
else
  urlRepoKey="https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-states.csv"
  repoKey="p971074907"
fi

echo "" >> ./update-repo.log

echo "******************** " > ./update-repo.log
echo "resultado da atualização em: $(date)" >> ./update-repo.log
echo "urlRepoKey: $urlRepoKey" >> ./update-repo.log
echo "repoKey: $repoKey" >> ./update-repo.log

echo "" >> ./update-repo.log
echo "erro na atualização do repositório:" >> ./update-repo.log
echo "repositório $repoKey: $(curl -X POST -H 'Content-Type: application/json' -d '{"urlRepo":"'$urlRepoKey'"}' ncovid.natalnet.br/datamanager/repo)" >> ./update-repo.log

echo "unidade brl: $(curl -X POST -H 'Content-Type: application/json' -d '{"locale":"brl","columns":[0,1,2,3,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23],"separator":",","connective":"and","cValueIndex":[3,4],"cValue":["TOTAL","TOTAL"]}' ncovid.natalnet.br/datamanager/repo/p971074907/slice)" >> ./update-repo.log

echo "" >> ./update-repo.log
echo "erros na execução do particinamento do repositório atualizado: " >> ./update-repo.log
unidades=("ac" "al" "am" "ap" "ba" "ce" "df" "es" "go" "ma" "mt" "ms" "mg" "pa" "pb" "pr" "pe" "pi" "rj" "rn" "ro" "rs" "rr" "sc" "se" "sp" "to")
for unidade in ${unidades[@]}; do
  resultado=$(curl -X POST -H 'Content-Type: application/json' -d '{"locale":"brl:'"${unidade^h}"'","columns":[0,1,2,3,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23],"separator":",","connective":"and","cValueIndex":[3,4],"cValue":["'"${unidade^^}"'","TOTAL"]}' ncovid.natalnet.br/datamanager/repo/$repoKey/slice)
  echo "unidade $unidade: $resultado" >> ./update-repo.log
done

