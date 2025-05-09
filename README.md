# dynamodblock
DynamoDB Lock for AWS Lambdas is a pure Python library that implements a distributed locking mechanism using DynamoDB. It's designed for scenarios where multiple concurrent Lambda executions need to ensure that certain tasks are performed exclusively by a single instance.

The library supports configurable TTL, retry logic with backoff, customizable timeouts, and operates in the time zone of your choice. It also integrates seamlessly with CloudWatch, enabling detailed logging for monitoring and debugging.

---

DynamoDB Lock for AWS Lambdas é uma biblioteca 100% Python que implementa um mecanismo de lock distribuído utilizando DynamoDB. Ideal para cenários com múltiplas execuções concorrentes de funções Lambda, ela garante que determinadas tarefas sejam executadas por apenas uma instância por vez.

A biblioteca oferece suporte a TTL configurável, retentativas com backoff, timeouts personalizáveis e opera no fuso horário que você escolher. Além disso, integra facilmente com o CloudWatch, permitindo logs detalhados para monitoramento e depuração.
