import asyncio
import sys
import os

# 프로젝트 루트를 sys.path에 추가
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

async def test_integrated_workflow_compatibility():
    print("Testing IntegratedWorkflow compatibility with app.py...")
    
    try:
        from backend.file.merge_workflow import IntegratedWorkflow
        
        # 1. 인스턴스 생성 및 속성 확인
        workflow = IntegratedWorkflow()
        print("Successfully instantiated IntegratedWorkflow.")
        
        # 속성 존재 여부 확인
        required_attrs = ['crawler_file_buffer', 'job_id', 'chat_bot_id', 'db_name', 'progress_callback']
        for attr in required_attrs:
            if hasattr(workflow, attr):
                print(f"  Attribute '{attr}' exists.")
            else:
                print(f"  ERROR: Attribute '{attr}' is MISSING!")
                return False
        
        # 2. start_workflow 시그니처 확인 (키워드 인자 테스트)
        # 실제 실행은 하지 않고 호출 가능 여부만 체크 (Mocking이나 짧은 타임아웃 필요할 수 있음)
        print("Checking start_workflow signature...")
        # Note: start_workflow is async and starts a loop, so we won't actually call it here 
        # but we can check the signature using inspect if needed.
        import inspect
        sig = inspect.signature(workflow.start_workflow)
        print(f"  start_workflow signature: {sig}")
        
        # 3. process_scan_batch 메서드 확인
        if hasattr(workflow, 'process_scan_batch'):
            print("  Method 'process_scan_batch' exists.")
            # 가짜 데이터로 호출 테스트 (JobQueues가 없으므로 내부에서 리턴될 것)
            await workflow.process_scan_batch([{'url': 'http://example.com/file.pdf'}])
            print("  Successfully called process_scan_batch with mock data.")
        else:
            print("  ERROR: Method 'process_scan_batch' is MISSING!")
            return False
            
        print("\nAll compatibility checks PASSED!")
        return True
        
    except Exception as e:
        print(f"\nVerification FAILED with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    asyncio.run(test_integrated_workflow_compatibility())
