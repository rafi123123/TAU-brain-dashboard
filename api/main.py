from logger_config import get_logger
from fastapi import FastAPI, BackgroundTasks
from cron import update_db, init_and_commit
import sql
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logger = get_logger(__name__)

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up FastAPI application")
    await init_and_commit()

    scheduler.add_job(
        update_db,
        trigger="cron",
        minute="*/15",  
        id="my_cron_job",
        replace_existing=False
    )
    
    scheduler.start()
    logger.info("Scheduler started successfully.")
    
    yield  # Application processes requests here
    
    logger.info("Shutting down FastAPI application")
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")
    await sql.close_pool()


app = FastAPI(title="Missing Folders API", lifespan=lifespan)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/update", response_class=HTMLResponse)
async def update_db_endpoint(background_tasks: BackgroundTasks):
    "updating the DB manually"
    background_tasks.add_task(update_db)
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Status</title>
        <style>
            body { font-family: system-ui, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background: #0f172a; color: #f8fafc; }
            .card { background: #1e293b; padding: 2rem 3rem; border-radius: 1rem; box-shadow: 0 10px 25px rgba(0,0,0,0.3); text-align: center; border: 1px solid #334155; }
            .dot { height: 12px; width: 12px; background-color: #38bdf8; border-radius: 50%; display: inline-block; animation: pulse 1.5s infinite; margin-right: 8px; }
            .subtext { margin-top: 1rem; font-size: 0.875rem; color: #94a3b8; }
            @keyframes pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.4; transform: scale(0.85); } }
        </style>
    </head>
    <body>
        <div class="card">
            <h2><span class="dot"></span>Currently updating the DB...</h2>
            <p class="subtext">This window will close in <span id="timer">5</span> seconds.</p>
        </div>

        <script>
            let seconds = 5;
            const timerElement = document.getElementById('timer');

            const countdown = setInterval(() => {
                seconds--;
                if (timerElement) timerElement.textContent = seconds;

                if (seconds <= 0) {
                    clearInterval(countdown);
                    window.close();
                }
            }, 1000);
        </script>
    </body>
    </html>
    """
