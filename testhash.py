from werkzeug.security import check_password_hash

hash_from_turso = "scrypt:32768:8:1$B9bMBSfgvbD3NztY$8f7ee498875c165a4cc8bc1a752b4c3aacb3f35699d2fe3c7b973559a278fa44615da2c6449b9651c05580b2248246a3fa8b7dabaef51949aa031c009993419e"
plain_password = "tZ($Xpu@;SRhjylJIE+q)L$x9#"

print(check_password_hash(hash_from_turso, plain_password))
