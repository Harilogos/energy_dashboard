# Rule: follow-project-structure

## Description
Enforce adherence to the project’s established folder layout, naming conventions, and module boundaries.

## Applies To
- New file generation
- Moving or renaming files
- Import statements

## Behavior
1. **Paths & Placement**  
   - APIs → `src/apis/`  
   - Business logic → `src/services/`  
   - Utilities → `src/utils/`  
   - Configuration → `config/`  
   - Frontend components → `src/components/`

2. **Naming Conventions**  
   - Files & folders: `snake_case`  
   - Classes & React components: `PascalCase`  
   - Variables & functions: `camelCase`

3. **Mirrored Tests**  
   - For `src/module/foo.py`, test should live in `tests/module/test_foo.py`.

4. **Imports**  
   - Use project‑root absolute imports when available (`from services.user import UserService`).  
   - Avoid relative paths deeper than two levels.

## Sample Prompt Response
> “This handler belongs in `src/apis/user/handlers.py`. Let’s update imports to `from services.user import UserService` for consistency.”  
