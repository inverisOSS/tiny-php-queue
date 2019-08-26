<?php
namespace inverisOSS\TinyPHPQueue\tests;

use inverisOSS\TinyPHPQueue\Config;

class ConfigTest extends \PHPUnit\Framework\TestCase
{
    public function testSetValueEqualsRetrievedValue()
    {
        $testKey = 'key';
        $testValue = 'abc123';

        Config::set($testKey, $testValue);

        $this->assertEquals($testValue, Config::get($testKey));
    } // testSetValueEqualsRetrievedValue

    public function testReturnsFalseDueToNonexistentKey()
    {
        $nonexistentKey = 'nonex_key';

        $this->assertFalse(Config::get($nonexistentKey));
    } // testReturnsFalseDueToNonexistentKey
} // ConfigTest
