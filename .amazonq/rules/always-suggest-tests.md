# Rule: always-suggest-tests

## Description
Ensure every new or modified code path is accompanied by a recommendation for appropriate testing.

## Applies To
- Any generated function, class, or endpoint
- Refactoring suggestions that change behavior

## Behavior
1. **Test Types**  
   - **Unit tests** for pure functions or small classes.  
   - **Integration tests** for multi‑module workflows or I/O.  
   - **E2E tests** when user flows or API contracts are involved.

2. **Naming & Placement**  
   - Suggest `test_<functionality>_with_<scenario>` naming.  
   - Mirror source structure under `tests/`.

3. **Mocking & Fixtures**  
   - Recommend mocks for external services.  
   - Use fixtures for repeated setup.

## Sample Prompt Response
> “After generating `processOrder()`, please add `tests/order/test_process_order_success.py` and cover scenarios: valid order, out‑of‑stock, and invalid payment.”  
