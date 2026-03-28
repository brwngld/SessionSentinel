@echo off
cd /d "c:\Users\Bernard\Desktop\learning\SessionSentinel"
git add -A
git commit -m "Implement OWASP-compliant exception handling for ALTER TABLE statements

- Wrapped ALTER TABLE statements with specific exception handling
- Only catches 'duplicate column name' errors, re-raises all others
- Complies with OWASP, PEP 20, and CWE-390 guidelines
- Prevents silent failures that could mask security issues
- Fixes persistent 'duplicate column name: role' error on Vercel

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
git push
