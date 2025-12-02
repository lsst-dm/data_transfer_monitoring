SECRET_PATH ?= secret/rubin/usdf-embargo-dmz/dtm

get-secrets-from-vault:
	mkdir -p etc/.secrets/
	set -e; for i in usdf-sasquatch-kafka-dtm-password summit-s3-read-only-access-key summit-s3-read-only-secret-key; do vault kv get --field=$$i $(SECRET_PATH) > etc/.secrets/$$i ; done

clean-secrets:
	rm -rf etc/.secrets/

run-kustomize:
	kubectl kustomize .

dump: get-secrets-from-vault run-kustomize clean-secrets

run-apply:
	kubectl apply -k .

apply: get-secrets-from-vault run-apply clean-secrets