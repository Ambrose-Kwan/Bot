Nexus Checkout Framework

A high-concurrency, low-latency automated checkout and inventory monitoring engine engineered for precision drops (e.g., Vans US/EU). Nexus eliminates split-brain routing and connection leaks using an immutable Proxy Pre-allocation model paired with an event-driven Two-Stage Decoupled Pipeline.

⚡ Key Architectural Highlights

Two-Stage Decoupled Dispatcher (Stage 1: Pre-warm -> Stage 2: Burst):

Stage 1 (Pre-warm): Pre-allocates network routes, initializes isolated HTTP/TLS sessions, warms anti-bot tokens, and stages empty cart sessions concurrently.

Stage 2 (Burst Execution): Monitors poll target SKUs with CDN bypass and timestamp-based cache busters. The moment inventory is detected, an async EventBus broadcasts the payload to awaken pre-warmed workers within single-digit milliseconds.

Immunity to Split-Brain Routing: Proxies are strictly leased and bound to task configurations before VansSite and RobustAsyncSession are instantiated.

Structured Concurrency & Cleanup Reporting: Worker sessions and proxy leases are tracked by task. Cancellation cascades to pre-warm workers, cleanup is retried when a close or lease settlement fails, and cleanup failures are surfaced rather than silently marked successful.

Consistent Monitor Routing: The monitor uses a dedicated lease when a ProxyPool is configured. Without a pool, it reuses a single worker proxy route; mixed worker routes require an explicit monitor route or direct-route opt-in.

Resilient Proxy Management: Built-in sticky session routing, automatic cooldowns for 429 Too Many Requests / 403 Forbidden responses, and sweeper routines for residential proxy networks (Oxylabs, Bright Data, Smartproxy).

🏗 System Architecture

               +--------------------------------------+
               |          LiveTaskInput / CLI         |
               +--------------------------------------+
                                   |
                                   v
             [Stage 1: Proxy Pre-allocation & Pre-warm]
            +-------------------------------------------+
            |  ProxyPool.get_proxy() -> bound_proxy     |
            |  VansSite(config, buyer) [Session Init]   |
            |  worker.prewarm()                         |
            +-------------------------------------------+
                                   | (ready_workers)
                                   v
             [Stage 2: FastStockMonitor & EventBus]
  +-------------------------+             +-------------------------+
  |    FastStockMonitor     | --[Signal]->|   Global Event Bus      |
  |  (CDN Cache-Busting)    |             | (In-memory Queues)      |
  +-------------------------+             +-------------------------+
                                                       |
                       +-------------------------------+-------------------------------+
                       |                               |                               |
                       v                               v                               v
            +---------------------+         +---------------------+         +---------------------+
            |   Worker 1 Burst    |         |   Worker 2 Burst    |         |   Worker N Burst    |
            |  - Apply Variant    |         |  - Apply Variant    |         |  - Apply Variant    |
            |  - Concurrent ATC   |         |  - Concurrent ATC   |         |  - Concurrent ATC   |
            |  - Fast Checkout    |         |  - Fast Checkout    |         |  - Fast Checkout    |
            +---------------------+         +---------------------+         +---------------------+
                       |                               |                               |
                       +-------------------------------+-------------------------------+
                                                       |
                                                       v
                                    +-------------------------------------+
                                    |     Idempotent Cleanup / Release    |
                                    |     - Close RobustAsyncSession      |
                                    |     - Report Proxy Success/Penalty  |
                                    |     - ProxyPool.release_task()      |
                                    +-------------------------------------+


📁 Repository Structure

├── config/
│   └── tasks.json              # Task definitions, buyer profiles, and proxy list
├── core/
│   ├── events.py               # In-memory pub/sub EventBus & StockEventPayload
│   ├── monitor.py              # FastStockMonitor with CDN cache-busting
│   └── proxy.py                # Thread-safe ProxyPool & residential session manager
├── models/
│   └── profile.py              # Pydantic schemas (TaskConfig, BuyerProfile, ProxyConfig)
├── sites/
│   ├── base_site.py            # BaseSite abstract class, session contract, & CheckoutResult
│   └── vans_site.py            # Vans-specific ATC, CSRF injection, and checkout flows
├── tests/
│   ├── test_address_jigging.py # Address permutation tests
│   ├── test_checkout_and_3ds.py# Payment and 3DS challenge tests
│   ├── test_live_dispatcher.py # Pipeline, concurrency, and proxy lifecycle tests
│   └── test_vans_site.py       # Site adapter pre-warm and ATC mocks
├── main_live.py                # Pipeline entrypoint (TwoStageLiveDispatcher)
└── pyproject.toml / requirements.txt


🚀 Installation & Environment Setup

1. Clone & Set Up Virtual Environment

git clone https://github.com/your-org/nexus-checkout-framework.git
cd nexus-checkout-framework

# Create Python virtual environment
python -m venv .venv

# Activate environment
# On Windows (PowerShell):
.\.venv\Scripts\Activate.ps1
# On Linux / macOS:
source .venv/bin/activate


2. Install Dependencies & Development Tools

pip install --upgrade pip
pip install -r requirements.txt

# Install code quality tools (Ruff & Pytest)
pip install ruff pytest pytest-asyncio


⚙️ Configuration (config/tasks.json)

Configure tasks, shipping details, and proxies in config/tasks.json:

{
  "site_key": "vans",
  "target_url_template": "https://www.vans.com/en-us/p/shoes/{sku}",
  "proxy_pool": [
    "http://user:password@node1.residential-proxy.com:8080",
    "http://user:password@node2.residential-proxy.com:8080"
  ],
  "buyers": [
    {
      "profile_id": "BUYER-001",
      "profile_name": "Primary Profile",
      "shipping": {
        "first_name": "Alex",
        "last_name": "Smith",
        "email": "alex.smith@example.com",
        "phone": "4155550199",
        "address1": "100 Market St",
        "address2": "Suite 400",
        "city": "San Francisco",
        "state_code": "CA",
        "postal_code": "94105",
        "country_code": "US"
      },
      "payment": {
        "cardholder_name": "Alex Smith",
        "card_number": "4111111111111111",
        "exp_month": 12,
        "exp_year": 2028,
        "cvv": "123"
      }
    }
  ]
}


🚦 Execution & Usage

Live Two-Stage Dispatcher

Run the dispatcher by targeting a specific product SKU:

python main_live.py \
  --sku "VN000D3HY28" \
  --sizes "10.5" "10" "11" \
  --concurrency 5 \
  --poll-interval-ms 1200 \
  --config config/tasks.json


CLI Parameters:

--sku (Required): Target Vans product SKU identifier.

--sizes (Optional): Desired size precedence order (e.g. 10.5 10 11).

--concurrency (Optional, Default: 5): Semaphore ceiling for concurrent pre-warm tasks and checkout workers.

--poll-interval-ms (Optional, Default: 1200): Polling frequency of the inventory monitor.

--config (Optional, Default: config/tasks.json): Path to task configuration file.

🧪 Testing & Code Quality

Run tests and lint checks locally:

# Run unit & integration tests
pytest tests/ -v

# Run live dispatcher isolation tests specifically
pytest tests/test_live_dispatcher.py -v

# Execute Ruff lint checks
ruff check .

# Auto-fix fixable formatting and lint issues
ruff check . --fix


🛡 Reliability & Error Handling Standards

BaseException & CancelledError Discipline: asyncio.CancelledError is explicitly caught and re-raised across all dispatcher routines to ensure clean cancellation workflows without dangling background tasks.

Decoupled Timeout Domains: Monitor timeouts (wait_for(queue.get())) and checkout execution errors are isolated. Checkout gateway lag is never masked as a stock broadcast timeout.

Unified Stage 2 Deadline: The configured wait budget covers stock waiting, checkout semaphore acquisition, and checkout execution. A checkout that exceeds the remaining budget is cancelled and reported as a deadline failure.

Evidence-Based Proxy Health: Structured rate-limit/WAF signals and explicit proxy/transport failures can trigger cooldowns. Generic timeouts, parsing errors, and repeated monitor errors alone do not mark a proxy as restricted.