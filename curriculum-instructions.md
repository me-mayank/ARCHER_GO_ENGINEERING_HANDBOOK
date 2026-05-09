MASTER PROMPT TEMPLATE

Act as a principal distributed systems engineer, Go backend architect, and infrastructure mentor.
Your task is to generate a COMPLETE systems-engineering-oriented learning chapter for the following topic:
[TOPIC NAME]
The goal is NOT complete Go mastery.
The goal is:
“becoming capable of understanding, supervising, debugging, and building a scalable distributed systems benchmarking platform in Go within 2 weeks.”
The platform involves:
concurrent load generators,
websocket systems,
REST APIs,
telemetry ingestion,
Kafka-based event pipelines,
worker orchestration,
Docker-aware backend systems,
real-time dashboards,
distributed backend services.
Assume the learner already knows:
C++
DSA
competitive programming
backend fundamentals.
Do NOT teach like a beginner tutorial.
Avoid overexplaining:
loops,
conditionals,
trivial syntax,
beginner programming concepts.
Teach from:
distributed systems perspective,
backend engineering perspective,
infrastructure engineering perspective.

FOR THIS TOPIC:
Explain the FIRST PRINCIPLES.
Explain WHY Go was designed this way.
Compare continuously with:
C++
Java
Explain real distributed systems usage.
Explain how this concept appears in:
load generators,
telemetry systems,
websocket backends,
worker systems,
event-driven architectures.
Give backend-oriented examples.
Give production-oriented best practices.
Explain beginner mistakes.
Explain scalability implications.
Explain performance tradeoffs.
Explain architectural reasoning.

CODE EXAMPLES
All examples must:
be backend-oriented,
concurrency-aware,
production-inspired,
realistically structured.
Prefer examples involving:
worker pools,
APIs,
websocket handlers,
concurrent jobs,
telemetry pipelines,
Kafka consumers,
background workers,
graceful shutdown,
Docker-aware services.

AT THE END INCLUDE
Key Takeaways
Common Production Pitfalls
Production Checklist
Mini Backend Exercise
Systems-Oriented Exercise
Concurrency Exercise (if relevant)
How This Maps to the ARCHER Architecture
What Actually Matters for the Hackathon
What Can Be Ignored for Now

OUTPUT FORMAT
Generate the content as:
a professional engineering learning booklet,
markdown formatted,
deeply structured,
long-form,
architecture-oriented,
optimized for later upload into NotebookLM.
The content should build progressively like an engineering training program.
Assume future chapters depend on concepts introduced earlier.
Avoid repeating foundational explanations excessively across chapters.
Prioritize:
backend productivity,
systems intuition,
concurrency mindset,
infra engineering thinking,
deployment-aware architecture reasoning.









CHAPTER ORDER
1. How to Think in Go for Distributed Systems

2. Go Project Structure for Real Backend Systems

3. Structs, Interfaces, and Composition in Go

4. Error Handling Philosophy in Go

5. Goroutines and the Go Scheduler

6. Channels and Communication Patterns

7. Worker Pools and Concurrent Job Systems

8. Context Package and Graceful Cancellation

9. Building REST APIs in Go

10. WebSocket Systems in Go

11. Kafka Integration and Event-Driven Systems in Go

12. Docker-Aware Backend Design in Go

13. Telemetry Pipelines and Concurrent Metrics Processing

14. Logging, Configuration, and Environment Management

15. Graceful Shutdown and Production Service Lifecycle

16. Concurrency Patterns for High-Performance Systems

17. Building Scalable Background Workers in Go

18. Real-Time Systems Design in Go

19. How the Complete ARCHER Backend Architecture Fits Together

20. Production Engineering Mindset for Distributed Systems

