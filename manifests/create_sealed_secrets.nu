#!/usr/bin/env nu

# Create Sealed Secrets for Dagster Crypto Data Code Location
# Usage: sops exec-env .env "nu create_sealed_secrets.nu"
# Requires: kubectl, kubeseal, sops

def main [] {
    print "🔐 Creating Sealed Secrets for Dagster Crypto Data..."
    
    # Check required environment variables
    let required_vars = [
        "DB_USERNAME",
        "DB_PASSWORD", 
        "S3_USER",
        "S3_PASSWORD"
    ]
    
    for var in $required_vars {
        if ($env | get -i $var | is-empty) {
            print $"❌ Error: ($var) environment variable not set"
            exit 1
        }
    }
    
    print "✅ All required environment variables found"
    
    # Create sealed secrets directory if it doesn't exist
    mkdir sealed-secrets
    
    # Create dagster-crypto-secrets sealed secret
    print "📦 Creating dagster-crypto-secrets..."
    
    kubectl create secret generic dagster-crypto-secrets `
        --from-literal=DB_USERNAME=$env.DB_USERNAME `
        --from-literal=DB_PASSWORD=$env.DB_PASSWORD `
        --from-literal=S3_USER=$env.S3_USER `
        --from-literal=S3_PASSWORD=$env.S3_PASSWORD `
        --namespace dagster `
        --dry-run=client -o yaml |
    kubeseal -o yaml |
    save -f sealed-secrets/dagster-crypto-secrets-sealed.yaml
    
    print "✅ Sealed secret created: sealed-secrets/dagster-crypto-secrets-sealed.yaml"
    print ""
    print "🎉 Done! You can now safely commit the sealed secrets to Git."
    print ""
    print "Next steps:"
    print "  1. Update kustomization.yaml to reference sealed-secrets/dagster-crypto-secrets-sealed.yaml"
    print "  2. Deploy: kubectl apply -k k8s/"
}
