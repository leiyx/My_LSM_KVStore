#include "gtest/gtest.h"
#include "db/memtable/memtable.h"
#include "include/env.h"
#include "iostream"

namespace lsmkv {
 
    TEST(MemTableTest, InternalKeyCmp) {
        InternalKeyComparator cmp(DefaultComparator());
        InternalKey a(0, std::string_view("abc"), KTypeInsertion);
        InternalKey b(1, std::string_view("abc"), KTypeInsertion);
        ASSERT_EQ(cmp.Compare(a,b), +1);
        a = InternalKey(0, std::string_view("abc"), KTypeInsertion);
        b = InternalKey(1, std::string_view("bcd"), KTypeInsertion);
        ASSERT_LT(cmp.Compare(a,b), 0);
    }

    TEST(MemTableTest, MemtableInsertAndDelete) {
        InternalKeyComparator cmp(DefaultComparator());
        MemTable* mem = new MemTable(cmp);
        mem->Ref();
        mem->Put(0, KTypeInsertion, "a", "1");
        mem->Put(1, KTypeInsertion, "a", "2");
        mem->Put(2, KTypeInsertion, "a", "3");
        mem->Put(3, KTypeInsertion, "a", "4");
        // check the insert
        LookupKey key("a",4);
        std::string result;
        Status status;
        ASSERT_EQ(mem->Get(key,&result,&status), true);
        std::string str = result;
        ASSERT_EQ(str,std::string{"4"});
        //check tje delete
        mem->Put(4, KTypeDeletion, "a", "");
        ASSERT_EQ(mem->Get(key,&result,&status), true);
        ASSERT_EQ(status.IsNotFound(),true);
        // insert after delete
        LookupKey key1("a",5);
        mem->Put(5, KTypeInsertion, "a", "5");
        ASSERT_EQ(mem->Get(key1,&result,&status), true);
        ASSERT_EQ(result,std::string{"5"});
        // lower sequence get
        mem->Put(6, KTypeInsertion, "a", "6");
        ASSERT_EQ(mem->Get(key1,&result,&status), true);
        ASSERT_EQ(result,std::string{"5"});
        mem->Unref();
    }

    class ConcurrencyTester {
    public:
        ConcurrencyTester(int N) 
         : rng_(std::random_device{}()),
         data_(N), cv_(&mu_), env(DefaultEnv()),done_num_(0) {
            InternalKeyComparator cmp(DefaultComparator());
            mem_ = new MemTable(cmp);
            mem_->Ref();
        }
        ~ConcurrencyTester() { mem_->Unref(); }

        void Writer() {
            for(int i = 0; i < data_.size(); i++) {
                std::string val = std::to_string(rng_() % 100);
                data_[i] = val;
                mem_->Put(i, KTypeInsertion, std::to_string(i), val);
            }
            for(int i = 0; i < data_.size() / 10; i++) {
                uint32_t key_delete = rng_() % data_.size();
                data_[key_delete] = "DELETE";
                mem_->Put(i+data_.size(), KTypeDeletion, std::to_string(key_delete), "");
            }
        }
        
        void WaitDone(int n) {
            mu_.Lock();
            while (done_num_ < n) {
                cv_.Wait();
            }
            mu_.Unlock();
        }
        MemTable* mem_;
        std::mt19937 rng_;
        std::vector<std::string> data_;
        Mutex mu_;
        CondVar cv_;
        Env* env;
        int done_num_;
    };
    void Reader(void* arg) {
        //std::cout<<"Reader start"<<std::endl;
        ConcurrencyTester* tester = reinterpret_cast<ConcurrencyTester*>(arg);
        tester->env->SleepMicroseconds(tester->rng_() % 100);
        for (int iter = 0; iter < 10; iter++){
            for(int i = 0; i < tester->data_.size(); i++){
                LookupKey key(std::to_string(i),tester->data_.size() * 2);
                std::string result;
                Status status;
                ASSERT_EQ(tester->mem_->Get(key,&result,&status), true);
                if (tester->data_[i] == "DELETE") {
                    //std::cout<<"i = " << i << ": "<< tester->data_[i]<<std::endl;
                    ASSERT_EQ(status.IsNotFound(),true);
                } else {
                    //std::cout<<"i = " << i << ": "<< result.ToString()<<std::endl;
                    ASSERT_EQ(tester->data_[i], result);
                }
            }
        }
        tester->mu_.Lock();
        tester->done_num_++;
        tester->cv_.Signal();
        tester->mu_.Unlock();
        //std::cout<<"Reader done"<<std::endl;
    }
    TEST(MemTableTest, ConcurrenceTest) {
        ConcurrencyTester tester(1000);
        int reader = 10;
        tester.Writer();
        for (int i = 0; i < reader; i++) {
            std::thread t(Reader, &tester);
            t.detach();
        }
        //Reader(&tester);
        tester.WaitDone(reader);
        //std::cout<<"DONE"<<std::endl;
    }
}