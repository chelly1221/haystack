from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, List
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import logging
import asyncio
import os
import psutil
import platform
import socket
import requests
import subprocess
import re

router = APIRouter()

def check_document_access(doc_meta, sosok, site):
    """Check if user has access to document based on sosok/site permissions"""
    doc_sosok = doc_meta.get("sosok", "").strip()
    doc_site = doc_meta.get("site", "").strip()
    
    # Admin access - can see everything
    if sosok == "관리자" and site == "관리자":
        return True
    
    # Must match sosok
    if sosok and doc_sosok != sosok:
        return False
    
    # Check site access
    if site:
        # Check for department-wide access (e.g., "레이더관제부_전체")
        if site.endswith("_전체"):
            # Allow access to all sites in the same sosok
            return True
        else:
            # Exact site match required
            if doc_site != site:
                return False
    
    return True

def get_nvidia_gpu_info():
    """Get NVIDIA GPU information using nvidia-smi"""
    gpu_info = []
    
    try:
        # Check if nvidia-smi exists
        result = subprocess.run(['nvidia-smi', '--query-gpu=index,name,memory.total,memory.used,memory.free,utilization.gpu,temperature.gpu,power.draw,power.limit', 
                               '--format=csv,noheader,nounits'], 
                               capture_output=True, text=True, timeout=5)
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            for line in lines:
                if line.strip():
                    parts = [p.strip() for p in line.split(',')]
                    if len(parts) >= 9:
                        gpu_info.append({
                            'index': int(parts[0]),
                            'name': parts[1],
                            'memory': {
                                'total': float(parts[2]),
                                'used': float(parts[3]),
                                'free': float(parts[4]),
                                'percent': round((float(parts[3]) / float(parts[2])) * 100, 1) if float(parts[2]) > 0 else 0
                            },
                            'utilization': float(parts[5]),
                            'temperature': float(parts[6]) if parts[6] != '[N/A]' else None,
                            'power': {
                                'draw': float(parts[7]) if parts[7] != '[N/A]' else None,
                                'limit': float(parts[8]) if parts[8] != '[N/A]' else None
                            }
                        })
    except FileNotFoundError:
        # nvidia-smi not found
        logging.info("nvidia-smi not found - no NVIDIA GPU detected")
    except subprocess.TimeoutExpired:
        logging.warning("nvidia-smi timeout")
    except Exception as e:
        logging.error(f"Error getting GPU info: {str(e)}")
    
    return gpu_info

def get_uptime_string():
    """Get system uptime as a formatted string with days, hours, minutes, and seconds in Korean"""
    boot_time = datetime.fromtimestamp(psutil.boot_time())
    uptime = datetime.now() - boot_time
    
    days = uptime.days
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    # Format as "X일 X시간 X분 X초"
    result = ""
    if days > 0:
        result += f"{days}일 "
    if hours > 0:
        result += f"{hours}시간 "
    if minutes > 0:
        result += f"{minutes}분 "
    if seconds > 0 or result == "":
        result += f"{seconds}초"
    
    return result.strip()

def get_statistics_router(qdrant_client):
    
    @router.get("/statistics/")
    async def get_statistics(
        sosok: Optional[str] = Query(None),
        site: Optional[str] = Query(None)
    ):
        """Get comprehensive statistics for documents"""
        try:
            loop = asyncio.get_event_loop()
            docs = await loop.run_in_executor(None, lambda: document_store.filter_documents(filters={}))
            
            # Filter by permissions
            filtered_docs = []
            for doc in docs:
                meta = doc.meta or {}
                if check_document_access(meta, sosok, site):
                    filtered_docs.append(doc)
            
            # Initialize statistics
            stats = {
                "total_documents": 0,
                "total_sections": len(filtered_docs),
                "total_size": 0,
                "documents_by_type": defaultdict(int),
                "documents_by_sosok": defaultdict(int),
                "documents_by_site": defaultdict(int),
                "popular_tags": [],
                "recent_uploads": [],
                "uploads_by_date": defaultdict(int),
                "average_sections_per_document": 0,
                "total_users": 0,
                "file_sizes": [],
                "access_level": "normal"  # Add access level indicator
            }
            
            # Set access level
            if sosok == "관리자" and site == "관리자":
                stats["access_level"] = "admin"
            elif site and site.endswith("_전체"):
                stats["access_level"] = "department"
            
            # Track unique documents and users
            unique_files = set()
            unique_users = set()
            file_info = {}
            tag_counter = Counter()
            
            # Process documents
            for doc in filtered_docs:
                meta = doc.meta or {}
                file_id = meta.get("file_id", "")
                
                if file_id and file_id not in unique_files:
                    unique_files.add(file_id)
                    
                    # Document type
                    filename = meta.get("original_filename", "")
                    
                    # Remove UUID prefix from filename if present
                    # Pattern: 32-character hex string followed by underscore
                    import re
                    clean_filename = re.sub(r'^[a-f0-9]{32}_', '', filename)
                    
                    ext = os.path.splitext(clean_filename.lower())[-1].lstrip('.')
                    if ext:
                        stats["documents_by_type"][ext] += 1
                    
                    # Sosok and Site
                    doc_sosok = meta.get("sosok", "Unknown")
                    doc_site = meta.get("site", "Unknown")
                    if doc_sosok:
                        stats["documents_by_sosok"][doc_sosok] += 1
                    if doc_site:
                        stats["documents_by_site"][doc_site] += 1
                    
                    # Store file info for recent uploads
                    # Extract date from document ID more reliably
                    upload_date = ""
                    if doc.id:
                        # Try to extract date from the ID
                        # Assuming ID format like "20240315_..." or contains date
                        import re
                        date_match = re.search(r'(\d{8})', doc.id)
                        if date_match:
                            upload_date = date_match.group(1)
                        elif len(doc.id) >= 8 and doc.id[:8].isdigit():
                            upload_date = doc.id[:8]
                    
                    # If no date in ID, try to get from metadata or use current date
                    if not upload_date and meta.get("upload_date"):
                        upload_date = meta.get("upload_date")
                    elif not upload_date:
                        # Try to get file creation time from disk
                        try:
                            upload_dir = "./uploads"
                            if file_id and os.path.exists(upload_dir):
                                for filename in os.listdir(upload_dir):
                                    if file_id in filename:
                                        filepath = os.path.join(upload_dir, filename)
                                        file_stat = os.stat(filepath)
                                        # Use file creation time (or modification time as fallback)
                                        timestamp = file_stat.st_birthtime if hasattr(file_stat, 'st_birthtime') else file_stat.st_mtime
                                        from datetime import datetime
                                        upload_date = datetime.fromtimestamp(timestamp).strftime("%Y%m%d")
                                        break
                        except:
                            pass
                        
                        # Final fallback - use current date
                        if not upload_date:
                            from datetime import datetime
                            upload_date = datetime.now().strftime("%Y%m%d")
                    
                    file_info[file_id] = {
                        "filename": clean_filename,  # Use cleaned filename
                        "upload_date": upload_date,
                        "tags": meta.get("tags", ""),
                        "sosok": doc_sosok,
                        "site": doc_site,
                        "total_pages": meta.get("total_pdf_pages", 0)
                    }
                
                # Tags
                tags_str = meta.get("tags", "")
                if tags_str:
                    tags = [t.strip() for t in tags_str.split(",") if t.strip()]
                    tag_counter.update(tags)
            
            # Calculate statistics
            stats["total_documents"] = len(unique_files)
            
            # Popular tags (top 10)
            stats["popular_tags"] = [
                {"name": tag, "count": count} 
                for tag, count in tag_counter.most_common(10)
            ]
            
            # Recent uploads (last 10)
            sorted_files = sorted(
                file_info.items(), 
                key=lambda x: x[1]["upload_date"], 
                reverse=True
            )[:10]
            
            stats["recent_uploads"] = [
                {
                    "file_id": file_id,
                    "filename": info["filename"],
                    "upload_date": info["upload_date"],
                    "tags": info["tags"],
                    "sosok": info["sosok"],
                    "site": info["site"]
                }
                for file_id, info in sorted_files
            ]
            
            # Average sections per document
            if stats["total_documents"] > 0:
                stats["average_sections_per_document"] = round(
                    stats["total_sections"] / stats["total_documents"], 1
                )
            
            # Convert defaultdicts to regular dicts for JSON serialization
            stats["documents_by_type"] = dict(stats["documents_by_type"])
            stats["documents_by_sosok"] = dict(stats["documents_by_sosok"])
            stats["documents_by_site"] = dict(stats["documents_by_site"])
            stats["uploads_by_date"] = dict(stats["uploads_by_date"])
            
            return stats
            
        except Exception as e:
            logging.error(f"❌ Error getting statistics: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    @router.get("/statistics/uploads-by-date/")
    async def get_uploads_by_date(
        days: int = Query(30, description="Number of days to look back"),
        sosok: Optional[str] = Query(None),
        site: Optional[str] = Query(None)
    ):
        """Get upload statistics by date for chart visualization"""
        try:
            loop = asyncio.get_event_loop()
            docs = await loop.run_in_executor(None, lambda: document_store.filter_documents(filters={}))
            
            # Filter by permissions
            filtered_docs = []
            for doc in docs:
                meta = doc.meta or {}
                if check_document_access(meta, sosok, site):
                    filtered_docs.append(doc)
            
            # Track uploads by date
            uploads_by_date = defaultdict(set)  # date -> set of file_ids
            
            # Get current date
            today = datetime.now()
            start_date = today - timedelta(days=days)
            
            # Initialize all dates with 0
            date_counts = {}
            for i in range(days):
                date = start_date + timedelta(days=i)
                date_str = date.strftime("%Y-%m-%d")
                date_counts[date_str] = 0
            
            # Count unique documents per day
            for doc in filtered_docs:
                meta = doc.meta or {}
                file_id = meta.get("file_id", "")
                
                # Extract date from document ID or metadata
                upload_date = None
                if doc.id and len(doc.id) >= 8:
                    try:
                        # Try multiple date extraction methods
                        import re
                        date_match = re.search(r'(\d{8})', doc.id)
                        if date_match:
                            date_str = date_match.group(1)
                            doc_date = datetime.strptime(date_str, "%Y%m%d")
                            upload_date = doc_date
                        elif doc.id[:8].isdigit():
                            doc_date = datetime.strptime(doc.id[:8], "%Y%m%d")
                            upload_date = doc_date
                    except:
                        pass
                
                # Try metadata if ID extraction failed
                if not upload_date and meta.get("upload_date"):
                    try:
                        upload_date = datetime.strptime(meta["upload_date"], "%Y%m%d")
                    except:
                        pass
                
                if upload_date and upload_date >= start_date and file_id:
                    formatted_date = upload_date.strftime("%Y-%m-%d")
                    uploads_by_date[formatted_date].add(file_id)
            
            # Convert sets to counts
            for date, file_ids in uploads_by_date.items():
                if date in date_counts:
                    date_counts[date] = len(file_ids)
            
            # Prepare data for chart
            sorted_dates = sorted(date_counts.keys())
            
            return {
                "dates": sorted_dates,
                "counts": [date_counts[date] for date in sorted_dates],
                "total": sum(date_counts.values()),
                "access_level": "admin" if (sosok == "관리자" and site == "관리자") else "normal"
            }
            
        except Exception as e:
            logging.error(f"❌ Error getting uploads by date: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    @router.get("/statistics/storage/")
    async def get_storage_statistics(
        sosok: Optional[str] = Query(None),
        site: Optional[str] = Query(None)
    ):
        """Get storage statistics for uploaded files"""
        try:
            # Admin users can see all storage stats
            if sosok == "관리자" and site == "관리자":
                # Show all storage stats
                upload_dir = "./uploads"
                total_size = 0
                file_count = 0
                size_by_type = defaultdict(int)
                
                if os.path.exists(upload_dir):
                    for filename in os.listdir(upload_dir):
                        filepath = os.path.join(upload_dir, filename)
                        if os.path.isfile(filepath):
                            file_size = os.path.getsize(filepath)
                            total_size += file_size
                            file_count += 1
                            
                            # Get file extension
                            ext = os.path.splitext(filename.lower())[-1].lstrip('.')
                            if ext:
                                size_by_type[ext] += file_size
                
                return {
                    "total_size": total_size,
                    "total_size_mb": round(total_size / (1024 * 1024), 2),
                    "total_size_gb": round(total_size / (1024 * 1024 * 1024), 2),
                    "file_count": file_count,
                    "average_file_size": round(total_size / file_count, 2) if file_count > 0 else 0,
                    "size_by_type": dict(size_by_type),
                    "size_by_type_mb": {
                        ext: round(size / (1024 * 1024), 2) 
                        for ext, size in size_by_type.items()
                    },
                    "access_level": "admin"
                }
            else:
                # For non-admin users, show limited info
                return {
                    "message": "Storage statistics are only available for administrators",
                    "access_level": "restricted"
                }
            
        except Exception as e:
            logging.error(f"❌ Error getting storage statistics: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    @router.get("/statistics/servers/")
    async def get_server_statistics(
        sosok: Optional[str] = Query(None),
        site: Optional[str] = Query(None)
    ):
        """Get server monitoring statistics"""
        try:
            # Admin users can see server stats
            if sosok == "관리자" and site == "관리자":
                # AI Server (current server) statistics
                ai_server = {
                    "name": "AI Server",
                    "hostname": socket.gethostname(),
                    "ip_address": socket.gethostbyname(socket.gethostname()),
                    "status": "online",
                    "platform": platform.system(),
                    "platform_version": platform.version(),
                    "processor": platform.processor(),
                    "python_version": platform.python_version(),
                    "cpu": {
                        "count": psutil.cpu_count(),
                        "count_logical": psutil.cpu_count(logical=True),
                        "percent": psutil.cpu_percent(interval=1),
                        "freq_current": round(psutil.cpu_freq().current, 2) if psutil.cpu_freq() else 0,
                        "freq_max": round(psutil.cpu_freq().max, 2) if psutil.cpu_freq() else 0
                    },
                    "memory": {
                        "total": psutil.virtual_memory().total,
                        "available": psutil.virtual_memory().available,
                        "used": psutil.virtual_memory().used,
                        "percent": psutil.virtual_memory().percent,
                        "total_gb": round(psutil.virtual_memory().total / (1024**3), 2),
                        "used_gb": round(psutil.virtual_memory().used / (1024**3), 2),
                        "available_gb": round(psutil.virtual_memory().available / (1024**3), 2)
                    },
                    "disk": {
                        "total": psutil.disk_usage('/').total,
                        "used": psutil.disk_usage('/').used,
                        "free": psutil.disk_usage('/').free,
                        "percent": psutil.disk_usage('/').percent,
                        "total_gb": round(psutil.disk_usage('/').total / (1024**3), 2),
                        "used_gb": round(psutil.disk_usage('/').used / (1024**3), 2),
                        "free_gb": round(psutil.disk_usage('/').free / (1024**3), 2)
                    },
                    "network": {
                        "bytes_sent": psutil.net_io_counters().bytes_sent,
                        "bytes_recv": psutil.net_io_counters().bytes_recv,
                        "packets_sent": psutil.net_io_counters().packets_sent,
                        "packets_recv": psutil.net_io_counters().packets_recv,
                        "bytes_sent_mb": round(psutil.net_io_counters().bytes_sent / (1024**2), 2),
                        "bytes_recv_mb": round(psutil.net_io_counters().bytes_recv / (1024**2), 2)
                    },
                    "uptime": get_uptime_string(),
                    "boot_time": datetime.fromtimestamp(psutil.boot_time()).isoformat(),
                    "current_time": datetime.now().isoformat()
                }
                
                # Add GPU information
                gpu_info = get_nvidia_gpu_info()
                if gpu_info:
                    ai_server["gpu"] = {
                        "available": True,
                        "count": len(gpu_info),
                        "devices": gpu_info
                    }
                else:
                    ai_server["gpu"] = {
                        "available": False,
                        "count": 0,
                        "devices": []
                    }
                
                # Process information
                processes = []
                for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_info']):
                    try:
                        pinfo = proc.info
                        if pinfo['cpu_percent'] > 1 or pinfo['memory_info'].rss > 100 * 1024 * 1024:  # CPU > 1% or Memory > 100MB
                            processes.append({
                                'pid': pinfo['pid'],
                                'name': pinfo['name'],
                                'cpu_percent': round(pinfo['cpu_percent'], 2),
                                'memory_mb': round(pinfo['memory_info'].rss / (1024**2), 2)
                            })
                    except:
                        pass
                
                # Sort by CPU usage
                processes.sort(key=lambda x: x['cpu_percent'], reverse=True)
                ai_server['top_processes'] = processes[:10]  # Top 10 processes
                
                # WEB Server statistics (external check)
                web_server = {
                    "name": "WEB Server",
                    "url": "http://localhost",  # Update with actual web server URL
                    "status": "unknown",
                    "response_time": None,
                    "status_code": None
                }
                
                # Try to check web server status
                try:
                    web_server_url = os.environ.get('WEB_SERVER_URL', 'http://localhost')
                    start_time = datetime.now()
                    response = requests.get(web_server_url, timeout=5)
                    end_time = datetime.now()
                    
                    web_server['url'] = web_server_url
                    web_server['status'] = 'online' if response.status_code == 200 else 'error'
                    web_server['status_code'] = response.status_code
                    web_server['response_time'] = int((end_time - start_time).total_seconds() * 1000)  # milliseconds
                except requests.exceptions.Timeout:
                    web_server['status'] = 'timeout'
                except requests.exceptions.ConnectionError:
                    web_server['status'] = 'offline'
                except Exception as e:
                    web_server['status'] = 'error'
                    web_server['error'] = str(e)
                
                # Vector Store statistics with unique document count
                loop = asyncio.get_event_loop()
                all_docs = await loop.run_in_executor(None, lambda: document_store.filter_documents(filters={}))
                
                # Count unique documents by file_id
                unique_doc_ids = set()
                for doc in all_docs:
                    if doc.meta and doc.meta.get("file_id"):
                        unique_doc_ids.add(doc.meta["file_id"])
                
                # Determine vector store type from document_store class name
                store_type = type(document_store).__name__
                if 'Qdrant' in store_type:
                    vector_type = 'Qdrant'
                elif 'FAISS' in store_type:
                    vector_type = 'FAISS'
                elif 'Elasticsearch' in store_type:
                    vector_type = 'Elasticsearch'
                elif 'Weaviate' in store_type:
                    vector_type = 'Weaviate'
                elif 'Pinecone' in store_type:
                    vector_type = 'Pinecone'
                else:
                    vector_type = store_type
                
                vector_store = {
                    "name": "Vector Store",
                    "type": vector_type,
                    "total_vectors": len(all_docs),  # Total number of vectors (sections)
                    "unique_documents": len(unique_doc_ids),  # Number of unique documents
                    "document_count": len(unique_doc_ids),  # For backward compatibility
                    "status": "online"
                }
                
                # Add Qdrant-specific information if available
                if 'Qdrant' in store_type and hasattr(document_store, 'client'):
                    try:
                        # Try to get collection info
                        if hasattr(document_store, 'index'):
                            vector_store['collection'] = document_store.index
                    except:
                        pass
                
                return {
                    "ai_server": ai_server,
                    "web_server": web_server,
                    "vector_store": vector_store,
                    "access_level": "admin"
                }
            else:
                # For non-admin users, show limited info
                return {
                    "message": "Server statistics are only available for administrators",
                    "access_level": "restricted"
                }
            
        except Exception as e:
            logging.error(f"❌ Error getting server statistics: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    @router.get("/health")
    async def health_check():
        """Simple health check endpoint"""
        return {"status": "healthy", "timestamp": datetime.now().isoformat()}
    
    
    return router