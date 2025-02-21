"""
Домашнее задание №4
Асинхронная работа с сетью и бд

доработайте функцию main, по вызову которой будет выполняться полный цикл программы
(добавьте туда выполнение асинхронной функции async_main):
- создание таблиц (инициализация)
- загрузка пользователей и постов
    - загрузка пользователей и постов должна выполняться конкурентно (параллельно)
      при помощи asyncio.gather (https://docs.python.org/3/library/asyncio-task.html#running-tasks-concurrently)
- добавление пользователей и постов в базу данных
  (используйте полученные из запроса данные, передайте их в функцию для добавления в БД)
- закрытие соединения с БД
"""
import logging
import asyncio
from typing import List, Dict

from sqlalchemy.ext.asyncio import AsyncEngine

from jsonplaceholder_requests import fetch_users_data, fetch_posts_data
from models import engine, Base, Session, User, Post




logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def create_tables(engine: AsyncEngine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def add_users_to_db(session: Session, users_data: List[Dict]):
    for user_data in users_data:
        filtered_user_data = {
            "id": user_data["id"],
            "name": user_data["name"],
            "username": user_data["username"],
            "email": user_data["email"],
        }
        user = User(**filtered_user_data)
        session.add(user)
    await session.commit()

async def add_posts_to_db(session: Session, posts_data: List[Dict]):
    for post_data in posts_data:
        filtered_post_data = {
            "id": post_data["id"],
            "user_id": post_data["userId"],
            "title": post_data["title"],
            "body": post_data["body"],
        }
        post = Post(**filtered_post_data)
        session.add(post)
    await session.commit()

async def async_main():
    try:
        logger.info("Creating tables...")
        await create_tables(engine)

        logger.info("Fetching users and posts data...")
        users_data, posts_data = await asyncio.gather(
            fetch_users_data(),
            fetch_posts_data(),
        )

        async with Session() as session:
            logger.info("Adding users to the database...")
            await add_users_to_db(session, users_data)
            logger.info("Adding posts to the database...")
            await add_posts_to_db(session, posts_data)

        logger.info("Data successfully added to the database.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        logger.info("Closing database connection...")
        await engine.dispose()

#    await create_tables(engine)
#    users_data, posts_data = await asyncio.gather(
#        fetch_users_data(),
#        fetch_posts_data(),
#    )
#    async with Session() as session:
#        await add_users_to_db(session, users_data)
#        await add_posts_to_db(session, posts_data)
#    await engine.dispose()

def main():
    asyncio.run(async_main())

if __name__ == "__main__":
    main()

