# Aide installation OpenCode sur les machines de l'ISIMA

Sur votre VM de l'ISIMA ou une machine linux/Mac OS : 

- installer opencode cf https://opencode.ai/ en faisant : 

```bash
curl -fsSL https://opencode.ai/v2/install | bash
```

- récupérez une clé d'API de notre infrastructure en allant sur https://ia.limos.fr 

Vous aurez une clé dans le style "sk-fdsiuho-fdsfdsifhdsfg". 

- si votre opencode n'est pas dans la liste des exécutable directement accessible faites : 

```bash
export PATH="$HOME/.opencode/bin:$PATH"
```

- Il faut configurer correctement le fichier de config d'OpenCode : 


```bash
mkdir -p ~/.config/opencode
touch ~/.config/opencode/opencode.json
chmod 600 ~/.config/opencode/opencode.json
```

le fichier ~/.config/opencode/opencode.json peut par exemple ressembler à :

```json
{
  "$schema": "https://opencode.ai/config.json",

  "model": "litellm/GLM-5.3-Flash",
  "small_model": "litellm/dev-model",
  "share": "disabled",

  "agent": {
    "build": {
      "model": "litellm/GLM-5.3-Flash"
    },
    "plan": {
      "model": "litellm/GLM-5.3-Flash"
    },
    "general": {
      "model": "litellm/GLM-5.3-Flash"
    }
  },

  "provider": {
    "litellm": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "Litellm",
      "options": {
        "baseURL": "https://api.ia.limos.fr/v1",
        "apiKey": "VOTRE CLE API de ia.limos.fr",
        "timeout": 600000,
        "chunkTimeout": 60000
      },
      "models": {
        "general": {
          "name": "general"
        },
        "general_nothink": {
          "name": "general_nothink"
        },
        "dev-model": {
          "name": "dev-model",
          "limit": {
            "context": 196608,
            "input": 155000,
            "output": 8192
          },
          "options": {
            "temperature": 0,
            "max_tokens": 8192
          }
        },
        "GLM-5.3-Flash": {
          "name": "GLM-5.3-Flash",
          "limit": {
            "context": 196608,
            "input": 155000,
            "output": 8192
          }
        }
      }
    }
  },

  "compaction": {
    "auto": true,
    "prune": true,
    "tail_turns": 5,
    "preserve_recent_tokens": 16000,
    "reserved": 32768
  },

  "tool_output": {
    "max_lines": 300,
    "max_bytes": 32768
  },

  "watcher": {
    "ignore": [
      ".git/**",
      "node_modules/**",
      "dist/**",
      "build/**",
      ".venv/**",
      "venv/**",
      "__pycache__/**",
      ".pytest_cache/**",
      ".mypy_cache/**",
      ".ruff_cache/**",
      "coverage/**",
      ".turbo/**",
      "out/**",
      "target/**",
      ".DS_Store",
      "Thumbs.db",
      ".env",
      ".env.local",
      ".idea/**",
      ".vscode/**",
      "*.swp",
      "*.swo",
      "*.tmp",
      "*.bak",
      "*.pyc",
      "*.pyo",
      "*.log",
      "*.sqlite",
      "*.db",
      "*.parquet",
      "*.csv"
    ]
  }
}
```

Remplacez ```VOTRE CLE API de ia.limos.fr``` par votre clé d'API au bon endroit dans le fichier ci-dessus 

Vous vous mettez dans le répertoire du TP et vous lancez les commandes suivantes pour lancer opencode : 

```bash
export XDG_CACHE_HOME="$HOME/.cache"
mkdir -p "$XDG_CACHE_HOME"
opencode
```

- Réalisation des activités pédagogiques : 

Je vous conseille dans opencode de lancer la commande "/init" avant de lui dire "analyse le répertoire courant". 

Ensuite vous pouvez lui demander : "réalise l'activité numéro 1 et réponds aux questions". ***Cela n'a de sens que si vous avez envie de comprendre les réponses...*** 
