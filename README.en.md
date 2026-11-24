# pTrader

pTrader is a Python service that separates CTP market-data ingestion from order handling. Market-data consumers subscribe through Redis. Strategies use the local trader client to call the order service.

| Module | Role |
| --- | --- |
| [Market-data and trader services](infra/trade/service/ctp/) | CTP-facing entry points, service operations, and the client. |
| [Redis utilities](utils/buffer/) | Message transport and shared utilities. |
| [Original README](README.md) | Original deployment notes and attribution for the SWIG binding. |

## Project timeline

- **July 2022.** The available GitHub history records the working split between the market-data publisher, the local trader service, and its Python client. [2022 commit history](https://github.com/danni2019/pTrader/commits/main/)
- **November 2026.** Added this English project overview to make the original architecture and dates easier to review.

A reproduction requires Redis, compatible CTP bindings, and a test environment such as SimNow. Use a local test setup and supply configuration outside source control. The original README credits 景色 CTP for the SWIG approach. The project's [MIT license](LICENSE) covers its own source, and the vendor SDK and bindings have their own distribution terms.
