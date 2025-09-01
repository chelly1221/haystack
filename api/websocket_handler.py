from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from typing import Dict, Set
import asyncio
import logging
import json
from task_manager_sqlite import task_manager

router = APIRouter()

class ConnectionManager:
    def __init__(self):
        # sosok_site를 키로 하는 WebSocket 연결 관리
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self.lock = asyncio.Lock()
    
    async def connect(self, websocket: WebSocket, sosok: str, site: str):
        await websocket.accept()
        key = f"{sosok}_{site}"
        async with self.lock:
            if key not in self.active_connections:
                self.active_connections[key] = set()
            self.active_connections[key].add(websocket)
        logging.info(f"🔌 WebSocket connected for {key}")
    
    async def disconnect(self, websocket: WebSocket, sosok: str, site: str):
        key = f"{sosok}_{site}"
        async with self.lock:
            if key in self.active_connections:
                self.active_connections[key].discard(websocket)
                if not self.active_connections[key]:
                    del self.active_connections[key]
        logging.info(f"🔌 WebSocket disconnected for {key}")
    
    async def send_to_site(self, sosok: str, site: str, message: dict):
        """특정 현장의 모든 연결에 메시지 전송"""
        key = f"{sosok}_{site}"
        async with self.lock:
            if key in self.active_connections:
                # 연결이 끊긴 소켓 추적
                disconnected = set()
                for connection in self.active_connections[key]:
                    try:
                        await connection.send_json(message)
                    except:
                        disconnected.add(connection)
                
                # 끊긴 연결 제거
                for conn in disconnected:
                    self.active_connections[key].discard(conn)
    
    async def broadcast_task_update(self, task_id: str):
        """작업 업데이트를 해당 현장에 브로드캐스트"""
        task = task_manager.get_task(task_id)
        if task:
            # 해당 현장의 모든 작업 목록 전송
            tasks = task_manager.get_tasks_by_site(task["sosok"], task["site"])
            await self.send_to_site(
                task["sosok"], 
                task["site"],
                {
                    "type": "task_update",
                    "tasks": tasks,
                    "updated_task_id": task_id
                }
            )

manager = ConnectionManager()

@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    sosok: str = Query(...),
    site: str = Query(...)
):
    await manager.connect(websocket, sosok, site)
    
    try:
        # 연결 시 현재 작업 목록 전송
        tasks = task_manager.get_tasks_by_site(sosok, site)
        await websocket.send_json({
            "type": "initial_tasks",
            "tasks": tasks
        })
        
        # Keep connection alive and send periodic updates
        while True:
            try:
                # Wait for any message from client (ping/pong)
                message = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
                
                # 클라이언트에서 보낸 메시지 처리
                try:
                    data = json.loads(message)
                    
                    # ping 메시지에 대한 pong 응답
                    if data.get("type") == "ping":
                        await websocket.send_json({"type": "pong"})
                    
                    # 작업 목록 새로고침 요청
                    elif data.get("type") == "refresh":
                        tasks = task_manager.get_tasks_by_site(sosok, site)
                        await websocket.send_json({
                            "type": "task_update",
                            "tasks": tasks
                        })
                    
                    # 작업 dismiss 요청
                    elif data.get("type") == "dismiss_task":
                        task_id = data.get("task_id")
                        if task_id:
                            task_manager.dismiss_task(task_id)
                            # 업데이트된 목록 전송
                            tasks = task_manager.get_tasks_by_site(sosok, site)
                            await websocket.send_json({
                                "type": "task_update",
                                "tasks": tasks
                            })
                    
                    # 모든 완료/실패 작업 dismiss
                    elif data.get("type") == "dismiss_all_completed":
                        task_manager.dismiss_completed_tasks(sosok, site)
                        # 업데이트된 목록 전송
                        tasks = task_manager.get_tasks_by_site(sosok, site)
                        await websocket.send_json({
                            "type": "task_update",
                            "tasks": tasks
                        })
                        
                except json.JSONDecodeError:
                    pass  # 잘못된 JSON 무시
                    
            except asyncio.TimeoutError:
                # 타임아웃 시 현재 작업 상태 전송 (heartbeat)
                tasks = task_manager.get_tasks_by_site(sosok, site)
                
                # 변경사항이 있는 경우에만 전송
                active_tasks = [t for t in tasks if t["status"] in ["uploading", "queued", "processing"]]
                if active_tasks:
                    await websocket.send_json({
                        "type": "task_update",
                        "tasks": tasks
                    })
                
                # 연결 확인용 ping
                try:
                    await websocket.send_json({"type": "ping"})
                except:
                    break  # 연결이 끊긴 경우
    
    except WebSocketDisconnect:
        await manager.disconnect(websocket, sosok, site)
    except Exception as e:
        logging.error(f"❌ WebSocket error: {e}")
        await manager.disconnect(websocket, sosok, site)

# Task Manager에서 상태 변경 시 WebSocket으로 알림을 보내는 함수
async def notify_task_update(task_id: str):
    """작업 상태 변경 시 해당 현장에 알림"""
    try:
        await manager.broadcast_task_update(task_id)
    except Exception as e:
        logging.error(f"❌ Failed to notify task update: {e}")

# TaskManager 업데이트 메서드들을 오버라이드하여 WebSocket 알림 추가
def setup_task_notifications():
    """Setup notifications for task updates"""
    original_update = task_manager.update_task_status
    original_complete = task_manager.complete_task
    original_fail = task_manager.fail_task
    original_dismiss = task_manager.dismiss_task
    original_dismiss_all = task_manager.dismiss_completed_tasks
    
    def update_with_notification(task_id: str, *args, **kwargs):
        result = original_update(task_id, *args, **kwargs)
        asyncio.create_task(notify_task_update(task_id))
        return result
    
    def complete_with_notification(task_id: str, *args, **kwargs):
        result = original_complete(task_id, *args, **kwargs)
        asyncio.create_task(notify_task_update(task_id))
        return result
    
    def fail_with_notification(task_id: str, *args, **kwargs):
        result = original_fail(task_id, *args, **kwargs)
        # 실패 시 즉시 알림 (사용자가 바로 확인할 수 있도록)
        asyncio.create_task(notify_task_update(task_id))
        return result
    
    def dismiss_with_notification(task_id: str):
        # task_id로부터 sosok, site 정보 가져오기
        task = task_manager.get_task(task_id)
        result = original_dismiss(task_id)
        if task:
            asyncio.create_task(manager.broadcast_task_update(task_id))
        return result
    
    def dismiss_all_with_notification(sosok: str, site: str):
        result = original_dismiss_all(sosok, site)
        # 임시 task_id로 브로드캐스트 (전체 목록 갱신)
        asyncio.create_task(async_broadcast_site_update(sosok, site))
        return result
    
    task_manager.update_task_status = update_with_notification
    task_manager.complete_task = complete_with_notification
    task_manager.fail_task = fail_with_notification
    task_manager.dismiss_task = dismiss_with_notification
    task_manager.dismiss_completed_tasks = dismiss_all_with_notification

async def async_broadcast_site_update(sosok: str, site: str):
    """특정 현장의 전체 작업 목록 업데이트 브로드캐스트"""
    tasks = task_manager.get_tasks_by_site(sosok, site)
    await manager.send_to_site(
        sosok, 
        site,
        {
            "type": "task_update",
            "tasks": tasks,
            "full_refresh": True
        }
    )

# Initialize notifications
setup_task_notifications()