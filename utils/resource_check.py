import psutil
import GPUtil
import platform
import datetime
import socket


def get_system_resources():
    # CPU 정보
    cpu_times = psutil.cpu_times()
    cpu_freq = psutil.cpu_freq()
    cpu_count = psutil.cpu_count(logical=False)
    cpu_count_logical = psutil.cpu_count(logical=True)
    cpu_percent = psutil.cpu_percent(interval=1, percpu=True)

    # 메모리 정보
    memory_info = psutil.virtual_memory()
    swap_info = psutil.swap_memory()

    # 디스크 정보
    disk_info = psutil.disk_usage("/")
    disk_io = psutil.disk_io_counters()

    # GPU 정보
    try:
        gpus = GPUtil.getGPUs()
        gpu_info = [
            {
                "id": gpu.id,
                "name": gpu.name,
                "load": gpu.load * 100,
                "memory_total": gpu.memoryTotal,
                "memory_used": gpu.memoryUsed,
                "memory_free": gpu.memoryFree,
                "temperature": gpu.temperature,
                "uuid": gpu.uuid,
            }
            for gpu in gpus
        ]
    except Exception as e:
        gpu_info = f"GPU 정보를 가져올 수 없습니다: {str(e)}"

    # 네트워크 정보
    net_io = psutil.net_io_counters()
    network_info = {
        "bytes_sent": net_io.bytes_sent,
        "bytes_recv": net_io.bytes_recv,
        "packets_sent": net_io.packets_sent,
        "packets_recv": net_io.packets_recv,
        "errin": net_io.errin,
        "errout": net_io.errout,
        "dropin": net_io.dropin,
        "dropout": net_io.dropout,
    }

    return {
        "cpu": {
            "times": {
                "user": cpu_times.user,
                "system": cpu_times.system,
                "idle": cpu_times.idle,
            },
            "frequency": {
                "current": cpu_freq.current,
                "min": cpu_freq.min,
                "max": cpu_freq.max,
            },
            "cpu_count": cpu_count,
            "cpu_count_logical": cpu_count_logical,
            "cpu_percent": cpu_percent,
        },
        "memory": {
            "total": memory_info.total,
            "available": memory_info.available,
            "used": memory_info.used,
            "percent": memory_info.percent,
            "cached": getattr(memory_info, "cached", None),
            "buffers": getattr(memory_info, "buffers", None),
            "shared": getattr(memory_info, "shared", None),
            "active": getattr(memory_info, "active", None),
            "inactive": getattr(memory_info, "inactive", None),
            "swap": {
                "total": swap_info.total,
                "used": swap_info.used,
                "free": swap_info.free,
                "percent": swap_info.percent,
                "sin": swap_info.sin,
                "sout": swap_info.sout,
            },
        },
        "disk": {
            "total": disk_info.total,
            "used": disk_info.used,
            "free": disk_info.free,
            "percent": disk_info.percent,
            "io": {
                "read_bytes": disk_io.read_bytes,
                "write_bytes": disk_io.write_bytes,
                "read_count": disk_io.read_count,
                "write_count": disk_io.write_count,
            },
        },
        "gpu": gpu_info,
        "network": network_info,
    }


def get_system_info():
    # 부팅 시간
    boot_time = datetime.fromtimestamp(psutil.boot_time()).strftime("%Y-%m-%d %H:%M:%S")
    return {
        "boot_time": boot_time,
        "users": psutil.users(),
        "processes": len(psutil.pids()),
        "system": platform.system(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
        "python_compiler": platform.python_compiler(),
        "python_build": platform.python_build(),
        "architecture": platform.architecture(),
        "uname": platform.uname(),
        "hostname": socket.gethostname(),
    }
