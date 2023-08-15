seed-db:
    #!/bin/bash
    mysql --protocol tcp -h$MYSQL_HOST -P$MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD $MYSQL_DB < scripts/mysql.sql
   