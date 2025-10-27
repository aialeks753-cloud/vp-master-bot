from collections import defaultdict
from datetime import datetime, timedelta
import logging

class RateLimiter:
    def __init__(self):
        self.user_requests = defaultdict(list)
    
    def check_limit(self, user_id: int, action: str, limit: int, period: int = 3600) -> bool:
        """
        Проверка лимита запросов
        user_id: ID пользователя
        action: тип действия (например, 'new_request', 'start_command')
        limit: максимальное количество запросов
        period: период в секундах (по умолчанию 1 час)
        """
        now = datetime.now()
        key = f"{user_id}_{action}"
        
        # Удаляем старые запросы (старше period секунд)
        self.user_requests[key] = [
            req_time for req_time in self.user_requests[key] 
            if now - req_time < timedelta(seconds=period)
        ]
        
        # Проверяем не превышен ли лимит
        if len(self.user_requests[key]) >= limit:
            return False
        
        # Добавляем текущий запрос
        self.user_requests[key].append(now)
        return True
    
    def get_remaining(self, user_id: int, action: str, limit: int, period: int = 3600) -> int:
        """Получить количество оставшихся запросов"""
        now = datetime.now()
        key = f"{user_id}_{action}"
        
        # Очищаем старые запросы
        self.user_requests[key] = [
            req_time for req_time in self.user_requests[key] 
            if now - req_time < timedelta(seconds=period)
        ]
        
        return max(0, limit - len(self.user_requests[key]))
    
    def get_time_until_reset(self, user_id: int, action: str, period: int = 3600) -> int:
        """Получить время до сброса лимита в секундах"""
        now = datetime.now()
        key = f"{user_id}_{action}"
        
        if not self.user_requests[key]:
            return 0
        
        # Время самого старого запроса
        oldest_request = min(self.user_requests[key])
        reset_time = oldest_request + timedelta(seconds=period)
        
        return max(0, int((reset_time - now).total_seconds()))
    
    def cleanup_old_entries(self):
        """Очистка старых записей (запускать периодически)"""
        now = datetime.now()
        keys_to_delete = []
        
        for key, timestamps in self.user_requests.items():
            # Удаляем записи старше 24 часов
            self.user_requests[key] = [
                t for t in timestamps 
                if now - t < timedelta(hours=24)
            ]
            # Если список пуст, помечаем ключ для удаления
            if not self.user_requests[key]:
                keys_to_delete.append(key)
        
        # Удаляем пустые ключи
        for key in keys_to_delete:
            del self.user_requests[key]
        
        logging.info(f"[RATE_LIMITER] Cleaned up {len(keys_to_delete)} old entries")