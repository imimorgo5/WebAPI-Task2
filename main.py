import asyncio
import random
from dataclasses import dataclass
from typing import Optional, List, Set

from playwright.async_api import async_playwright
from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ConfigDict
from sqlmodel import SQLModel, Field, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker


DATABASE_URL = "sqlite+aiosqlite:///./tasks.db"
BACKGROUND_INTERVAL_SECONDS = 60
BASE_URL = "https://www.letu.ru/browse/muzhchinam/muzhskaya-parfyumeriya"
PARSE_LIMIT = 10
MAX_PAGES = 100

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
async_session = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

parse_lock = asyncio.Lock()


def normalize(text):
    if not text:
        return ""
    return " ".join(text.replace("\u00A0", " ").replace("\xa0", " ").split()).strip()


@dataclass
class Product:
    title: str
    brand: str
    price: str
    url: str


class LetuParser:
    base_url = BASE_URL

    def __init__(self):
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    async def start(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()

    async def load_page(self, url):
        await self.page.goto(url, timeout=60000)
        await self.page.wait_for_selector("a[href*='/product/']", timeout=10000)

    async def parse_products_from_page(self):
        products = []
        anchors = await self.page.query_selector_all("a[href*='/product/']")
        for a in anchors:
            try:
                href = await a.get_attribute("href")
                link = "https://www.letu.ru" + href

                title_el = await a.query_selector(".product-tile-name__text > span:nth-child(3)")
                title = await title_el.text_content()

                brand_el = await a.query_selector(".product-tile-name__text--brand")
                brand = await brand_el.text_content()

                price_el = await a.query_selector(".product-tile-price__text--actual")
                price = await price_el.text_content()

                products.append(
                    Product(title=normalize(title), brand=normalize(brand), price=normalize(price), url=link))
            except Exception:
                continue
        return products

    async def stop(self):
        await self.browser.close()
        await self.playwright.stop()


class ParserState(SQLModel, table=True):
    key: str = Field(primary_key=True)
    value: int = Field(default=0)


class Task(SQLModel, table=True):
    model_config = ConfigDict(from_attributes=True)
    id: Optional[int] = Field(default=None, primary_key=True)
    title: str
    description: Optional[str] = ""
    done: bool = Field(default=False)


class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)

    async def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)

    async def broadcast(self, message: dict):
        for ws in list(self.active_connections):
            await ws.send_json(message)

    async def handle(self, data, websocket: WebSocket):
        await websocket.send_text(data)


async def get_db():
    async with async_session() as session:
        yield session


manager = ConnectionManager()

app = FastAPI(title="TODO API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_background_task_handle: Optional[asyncio.Task] = None
_shutdown_event = asyncio.Event()


@app.get("/tasks", response_model=List[Task])
async def api_list_tasks(session: AsyncSession = Depends(get_db)):
    result = await session.execute(select(Task).order_by(Task.id))
    return result.scalars().all()


@app.get("/tasks/{task_id}", response_model=Task)
async def api_get_task(task_id: int, session: AsyncSession = Depends(get_db)):
    task = await session.get(Task, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@app.post("/tasks", response_model=Task, status_code=201)
async def api_create_task(task_in: Task, session: AsyncSession = Depends(get_db)):
    task = Task(title=task_in.title, description=task_in.description, done=task_in.done)
    session.add(task)
    await session.commit()
    await session.refresh(task)
    await manager.broadcast({"event": "task_created", "task": Task.model_validate(task).model_dump()})
    return task


@app.patch("/tasks/{task_id}", response_model=Task)
async def api_patch_task(task_id: int, patch: Task, session: AsyncSession = Depends(get_db)):
    task = await session.get(Task, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if patch.title is not None:
        task.title = patch.title
    if patch.description is not None:
        task.description = patch.description
    if patch.done is not None:
        task.done = patch.done
    session.add(task)
    await session.commit()
    await session.refresh(task)
    await manager.broadcast({"event": "task_updated", "task": Task.model_validate(task).model_dump()})
    return task


@app.delete("/tasks/{task_id}", response_model=Task)
async def api_delete_task(task_id: int, session: AsyncSession = Depends(get_db)):
    task = await session.get(Task, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    await session.delete(task)
    await session.commit()
    await manager.broadcast({"event": "task_deleted", "task": Task.model_validate(task).model_dump()})
    return task


@app.websocket("/ws/tasks")
async def ws_tasks(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await manager.handle(data, websocket)
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(websocket)


async def parse_site(session: AsyncSession):
    parser = LetuParser()
    await parser.start()
    collected: List[Product] = []
    base = parser.base_url.rstrip("/")

    result = await session.execute(select(ParserState).where(ParserState.key == "page"))
    page_state = result.scalars().first()
    if not page_state:
        page_state = ParserState(key="page", value=1)
        session.add(page_state)
        await session.commit()
        await session.refresh(page_state)

    result = await session.execute(select(ParserState).where(ParserState.key == "index"))
    index_state = result.scalars().first()
    if not index_state:
        index_state = ParserState(key="index", value=0)
        session.add(index_state)
        await session.commit()
        await session.refresh(index_state)

    start_page = max(1, page_state.value)
    if start_page > MAX_PAGES:
        start_page = 1
    start_index = max(0, index_state.value)

    page_num = start_page

    page_lengths = {}

    next_page = start_page
    next_index = start_index

    while len(collected) < PARSE_LIMIT:
        if page_num > MAX_PAGES:
            page_num = 1

        page_url = f"{base}/page-{page_num}"
        try:
            await parser.load_page(page_url)
        except Exception:
            page_num += 1
            if page_num == start_page:
                break
            continue

        page_products = await parser.parse_products_from_page()
        page_lengths[page_num] = len(page_products)

        if not page_products:
            page_num += 1
            continue

        for i, p in enumerate(page_products):
            if page_num == start_page and i < start_index and len(collected) == 0:
                continue

            if len(collected) >= PARSE_LIMIT:
                break

            collected.append(p)

            next_page = page_num
            next_index = i + 1

        if next_page in page_lengths and next_index >= page_lengths[next_page]:
            next_page = page_num + 1
            next_index = 0

        page_num += 1

        if page_num - start_page > MAX_PAGES + 2:
            break

    await parser.stop()

    if next_page > MAX_PAGES:
        next_page = 1
        next_index = 0

    if not collected:
        next_page = start_page + 1
        next_index = 0
        if next_page > MAX_PAGES:
            next_page = 1

    return collected, next_page, next_index


async def run_task_generator_once(session: AsyncSession):
    async with parse_lock:
        items, next_page, next_index = await parse_site(session)
        if not items:
            result = await session.execute(select(ParserState).where(ParserState.key == "page"))
            page_state = result.scalars().first()
            if page_state:
                page_state.value = next_page
                session.add(page_state)

            result = await session.execute(select(ParserState).where(ParserState.key == "index"))
            index_state = result.scalars().first()
            if index_state:
                index_state.value = next_index
                session.add(index_state)

            await session.commit()
            return 0

        for p in items:
            title = f"Посмотреть на сайте letu.ru парфюм {p.brand} {p.title}"
            description = f"Цена: {p.price}\nСсылка: {p.url}"
            done_val = random.choice([True, False])
            task = Task(title=title, description=description, done=done_val)
            session.add(task)

        await session.commit()

        result = await session.execute(select(ParserState).where(ParserState.key == "page"))
        page_state = result.scalars().first()
        if page_state:
            page_state.value = next_page
            session.add(page_state)

        result = await session.execute(select(ParserState).where(ParserState.key == "index"))
        index_state = result.scalars().first()
        if index_state:
            index_state.value = next_index
            session.add(index_state)

        await session.commit()

        result = await session.execute(select(Task).order_by(Task.id.desc()).limit(len(items)))
        new_tasks = result.scalars().all()[::-1]
        added = 0
        for task in new_tasks:
            added += 1
            await manager.broadcast({"event": "task_created", "task": Task.model_validate(task).model_dump()})

        return added


async def background_loop(interval_seconds):
    while not _shutdown_event.is_set():
        async with async_session() as session:
            await run_task_generator_once(session)
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            continue


async def _run_once_in_session():
    async with async_session() as session:
        await run_task_generator_once(session)


@app.post("/task-generator/run")
async def api_force_run_generator():
    asyncio.create_task(_run_once_in_session())
    return {"message": "Генератор задач запущен в фоновом режиме"}


@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    global _background_task_handle
    if _background_task_handle is None:
        _background_task_handle = asyncio.create_task(background_loop(BACKGROUND_INTERVAL_SECONDS))


@app.on_event("shutdown")
async def on_shutdown():
    _shutdown_event.set()
    global _background_task_handle
    if _background_task_handle:
        await asyncio.shield(_background_task_handle)
    await engine.dispose()
