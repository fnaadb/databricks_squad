# Contributing to Databricks Squad

Thank you for contributing to this project!

## Branch Naming Convention

Use the following prefixes:

| Prefix | Purpose | Example |
|--------|---------|---------|
| `feature/` | New features | `feature/add-merchant-kpis` |
| `bugfix/` | Bug fixes | `bugfix/fix-null-handling` |
| `hotfix/` | Urgent production fixes | `hotfix/critical-data-fix` |
| `refactor/` | Code refactoring | `refactor/optimize-silver-joins` |
| `docs/` | Documentation updates | `docs/update-runbook` |
| `test/` | Test additions | `test/add-integration-tests` |

## Development Workflow

### 1. Create a Feature Branch

```bash
git checkout main
git pull origin main
git checkout -b feature/your-feature-name
```

### 2. Make Changes

- Follow the existing code style
- Add tests for new functionality
- Update documentation as needed

### 3. Test Locally

```bash
# Run unit tests
pytest tests/unit/ -v

# Run all tests
pytest tests/ -v

# Validate Databricks bundle
databricks bundle validate
```

### 4. Commit Changes

```bash
git add .
git commit -m "feat: description of your changes"
```

#### Commit Message Format

Use conventional commits:

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation
- `test:` Tests
- `refactor:` Code refactoring
- `chore:` Maintenance

### 5. Push and Create PR

```bash
git push origin feature/your-feature-name
gh pr create --fill
```

### 6. Code Review

- Wait for at least one approval
- Address review comments
- **Manual merge by repository owner only**

## Code Standards

### Python Style

- Follow PEP 8
- Use type hints
- Maximum line length: 100 characters
- Use meaningful variable names

### PySpark Best Practices

- Use DataFrame API (not RDDs)
- Chain transformations where readable
- Use column expressions over UDFs when possible
- Include schema validation

### Testing Requirements

- Unit tests for all transformation functions
- Integration tests for layer-to-layer flows
- Data quality tests for schema validation
- Minimum 80% code coverage for new code

### Documentation

- Docstrings for all public functions
- Update README for new features
- Document architectural decisions

## Pull Request Checklist

- [ ] Code follows project style guidelines
- [ ] Tests added/updated for changes
- [ ] Documentation updated
- [ ] PR description clearly explains changes
- [ ] Branch is up to date with main
- [ ] All CI checks pass

## Questions?

Open an issue for discussion before making large changes.
