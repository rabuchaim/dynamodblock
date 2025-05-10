# dynamodblock
DynamoDB Lock for AWS Lambdas is a pure Python library that implements a distributed locking mechanism using DynamoDB. It's designed for scenarios where multiple concurrent Lambda executions need to ensure that certain tasks are performed exclusively by a single instance.

The library supports configurable TTL, retry logic with backoff, customizable timeouts, and operates in the time zone of your choice. It also integrates seamlessly with CloudWatch, enabling detailed logging for monitoring and debugging.

<img src="https://raw.githubusercontent.com/rabuchaim/dynamodblock/refs/heads/main/code.png" />

Here we have 2 sessions with timezones 2 hours apart, competing for a lock with executions 1 second after each other. The lock has 10 seconds of retries with a 1 second interval between each attempt. If the initial lock remained active for 5 seconds, the subsequent lock will be able to perform acquire() before the 10 second timeout.

<img src="https://raw.githubusercontent.com/rabuchaim/dynamodblock/refs/heads/main/abuchaim-2025-05-10%2014_33_36.png" />
---

DynamoDB Lock for AWS Lambdas é uma biblioteca 100% Python que implementa um mecanismo de lock distribuído utilizando DynamoDB. Ideal para cenários com múltiplas execuções concorrentes de funções Lambda, ela garante que determinadas tarefas sejam executadas por apenas uma instância por vez.

A biblioteca oferece suporte a TTL configurável, retentativas com backoff, timeouts personalizáveis e opera no fuso horário que você escolher. Além disso, integra facilmente com o CloudWatch, permitindo logs detalhados para monitoramento e depuração.

Aqui temos 2 sessões com timezones com 2 horas de diferença, disputando um lock com execuções 1 segundo após a outra. O lock possui 10 segundos de retentativas com 1 segundo de intervalo entre cada tentativa. Se o lock inicial se manteve ativo por 5 segundos, o lock posterior vai conseguir fazer o acquire() antes dos 10 segundos de timeout.

<img src="https://raw.githubusercontent.com/rabuchaim/dynamodblock/refs/heads/main/abuchaim-2025-05-10%2014_33_36.png" />
