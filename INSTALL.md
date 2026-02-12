# Installation Guide

## Quick Install

```bash
# 1. Download the skill
curl -L https://github.com/user/omni-stack-agentic-architect/archive/main.zip -o skill.zip
unzip skill.zip

# 2. Install to Claude
mkdir -p ~/.claude/skills/user/
cp -r omni-stack-agentic-architect ~/.claude/skills/user/

# 3. Verify installation
ls ~/.claude/skills/user/omni-stack-agentic-architect/
```

You should see:
```
README.md
SKILL.md
skill.json
examples/
reference/
```

## Manual Installation

1. **Download files** from the package
2. **Create directory**: `mkdir -p ~/.claude/skills/user/omni-stack-agentic-architect`
3. **Copy all files** to that directory
4. **Restart Claude** (if needed)

## Verify It Works

Open a new conversation with Claude and try:

```
You: Build a simple multi-agent system that monitors GitHub PRs 
     and sends Slack notifications

Claude: I'll build a 3-agent orchestration system...
        [proceeds to build complete system]
```

If Claude uses the skill, you'll see:
- Structured architecture
- Complete implementations
- Production-ready code
- Observability built-in

## Troubleshooting

**Skill not activating?**
- Check file location: `~/.claude/skills/user/omni-stack-agentic-architect/`
- Verify SKILL.md exists
- Be explicit in your request: "build a multi-agent system" or "create a full-stack app"

**Wrong behavior?**
- Make sure you copied ALL files, not just SKILL.md
- Check that examples/ and reference/ directories exist

**Need help?**
- Read the examples in `examples/`
- Check reference documentation in `reference/`
- See README.md for usage patterns

## Updating

```bash
cd ~/.claude/skills/user/omni-stack-agentic-architect
git pull  # If installed from git

# Or download new version and replace files
```

## Uninstalling

```bash
rm -rf ~/.claude/skills/user/omni-stack-agentic-architect
```

---

**You're ready to build production-grade systems! 🚀**
