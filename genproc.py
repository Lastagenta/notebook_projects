import psutil
 
def get_running_proccesses() -> list:
    ''' Returns list of all running processes '''
    # получаем генератор для всех процессов
    processes_iter = psutil.process_iter()
 
    processes = []
 
    # добавляем объекты из генератора в список
    for process in processes_iter:
        processes.append(
            {
                'name': process.name(),
                'pid': process.pid,
                'memory used': process.memory_info().vms / 1024**2, 
            }
        )
 
    processes = sorted(processes, key=lambda d: d['memory used'], reverse=True) # сортируем процессы по ключу в словарях "memory used"
 
    return processes # возвращаем все процессы
