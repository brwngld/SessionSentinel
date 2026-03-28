@echo off
cd /d "c:\Users\Bernard\Desktop\learning\SessionSentinel"
git add -A
git commit -m "Guard ALTER TABLE statements to prevent duplicate column errors on Vercel libsql"
git push
